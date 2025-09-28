from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import os, time
from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy import inspect
from sqlalchemy.exc import OperationalError, InterfaceError

from functions.tables import student_table
from functions.datatable import data_table
from functions.clients import (
    get_github_summary,
    get_leetcode_profile,
    get_leetcode_language_stats,
    get_leetcode_badges,
    get_github_contributions,
    get_leetcode_calendar,
)
from functions.notification import (
    create_notification_table,
    remove_notification_with_reason,
    notification_table,
)


def _safe_int(v: Any) -> int | None:
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


def _join_list(vals: List[str] | None) -> str | None:
    if not vals:
        return None
    return ",".join(vals)


def compute_stats(
    git_json: Dict[str, Any],
    lc_prof: Dict[str, Any],
    lc_lang: Dict[str, Any],
    lc_badges: Dict[str, Any],
    git_contri: Dict[str, Any] | None = None,
    lc_calendar: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    followers = _safe_int(git_json.get("followers"))
    following = _safe_int(git_json.get("following"))
    public_repo = _safe_int(git_json.get("public_repo_count"))

    original_repos = git_json.get("original_repos") or {}
    authored_forks = git_json.get("authored_forks") or {}
    orig_count = len(original_repos)
    authored_count = len(authored_forks)

    last_commit_date = None
    overall = git_json.get("overall_last_commit") or {}
    # Normalize to date-only (YYYY-MM-DD) whether ISO string or unix timestamp
    raw_last_commit = overall.get("date")

    def _to_date_only(val: Any) -> str | None:
        try:
            if val is None:
                return None
            s = str(val).strip()
            if not s:
                return None
            if s.isdigit():
                # treat as unix seconds
                dt = datetime.fromtimestamp(int(s), tz=timezone.utc)
                return dt.strftime('%Y-%m-%d')
            # If ISO-like string, prefer first 10 chars when in YYYY-MM-DD format
            if len(s) >= 10 and s[4] == '-' and s[7] == '-':
                return s[:10]
            # best-effort parse
            try:
                dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
                return dt.date().isoformat()
            except Exception:
                return None
        except Exception:
            return None

    last_commit_date = _to_date_only(raw_last_commit)

    git_badges_list = []
    badges_map = git_json.get("badges") or {}
    if isinstance(badges_map, dict):
        git_badges_list = list(badges_map.keys())

    # LeetCode profile
    totalSolved = _safe_int(lc_prof.get("totalSolved"))
    easy = _safe_int(lc_prof.get("easySolved"))
    medium = _safe_int(lc_prof.get("mediumSolved"))
    hard = _safe_int(lc_prof.get("hardSolved"))
    ranking = _safe_int(lc_prof.get("ranking"))

    def _to_date_from_ts(ts: Any) -> str | None:
        try:
            if ts is None:
                return None
            s = str(ts).strip()
            if not s:
                return None
            if not s.isdigit():
                return None
            dt = datetime.fromtimestamp(int(s), tz=timezone.utc)
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return None

    lastsubmission = None
    lastacceptedsubmission = None
    recent = lc_prof.get("recentSubmissions") or []
    if recent:
        # Most recent overall submission (first item)
        ts = recent[0].get("timestamp")
        lastsubmission = _to_date_from_ts(ts)
        # Most recent accepted submission
        for sub in recent:
            if (sub.get("statusDisplay") or "").lower() == "accepted":
                ats = sub.get("timestamp")
                lastacceptedsubmission = _to_date_from_ts(ats)
                break

    # Languages
    lang_counts = lc_lang.get("matchedUser", {}).get("languageProblemCount", [])
    languages = [str(x.get("languageName")) for x in lang_counts if x.get("languageName")]

    # LeetCode badges: store badgesCount in lc_badges as requested
    lc_badges_count = _safe_int(lc_badges.get("badgesCount"))

    # LeetCode streaks from submissionCalendar (map of unixDay -> count)
    def _calc_streaks(sub_cal: Dict[str, Any]) -> tuple[int | None, int | None]:
        if not isinstance(sub_cal, dict) or not sub_cal:
            return None, None
        try:
            # Convert keys to ints (days in seconds), normalize to UTC dates
            days = set()
            for k, v in sub_cal.items():
                if v is None:
                    continue
                s = str(k).strip()
                if not s.isdigit():
                    continue
                ts = int(s)
                if ts <= 0:
                    continue
                d = datetime.fromtimestamp(ts, tz=timezone.utc).date()
                days.add(d)
            if not days:
                return 0, 0
            # Compute longest contiguous daily streak and current streak up to today (UTC)
            max_streak = 0
            cur_streak = 0
            d = min(days)
            end = max(days)
            one = timedelta(days=1)
            while d <= end:
                if d in days:
                    cur_streak += 1
                    max_streak = max(max_streak, cur_streak)
                else:
                    cur_streak = 0
                d = d + one
            # Current streak: count backwards from today if continuous
            today = datetime.now(tz=timezone.utc).date()
            cur = 0
            d = today
            while d in days:
                cur += 1
                d = d - one
            return cur, max_streak
        except Exception:
            return None, None

    from datetime import timedelta  # local import to avoid widening module imports
    # submission calendar may appear from profile or dedicated calendar endpoint
    sub_cal_raw = lc_prof.get("submissionCalendar") or {}
    # The calendar endpoint wraps it as string under 'submissionCalendar'
    if lc_calendar and isinstance(lc_calendar.get("submissionCalendar"), str):
        try:
            import json as _json
            sub_cal_raw = _json.loads(lc_calendar["submissionCalendar"]) or sub_cal_raw
        except Exception:
            pass
    lc_cur_streak, lc_max_streak = _calc_streaks(sub_cal_raw or {})

    # Normalize and store universal history formats
    # - LeetCode: map of YYYY-MM-DD -> count
    lc_history: Dict[str, int] | None = None
    try:
        if isinstance(sub_cal_raw, dict) and sub_cal_raw:
            lc_history = {}
            for k, v in sub_cal_raw.items():
                s = str(k).strip()
                if s.isdigit():
                    ts = int(s)
                    if ts > 10**10:  # milliseconds guard
                        ts = ts // 1000
                    d = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
                    lc_history[d] = int(v or 0)
    except Exception:
        lc_history = None

    # - GitHub: map of YYYY-MM-DD -> count from contributionDays
    gh_history: Dict[str, int] | None = None
    try:
        days: Dict[str, int] = {}
        if git_contri and isinstance(git_contri.get("weeks"), list):
            for w in git_contri["weeks"]:
                for d in (w.get("contributionDays") or []):
                    day = d.get("date")
                    cnt = d.get("contributionCount")
                    if day:
                        days[str(day)] = int(cnt or 0)
        gh_history = days if days else None
    except Exception:
        gh_history = None

    return {
        "git_followers": followers,
        "git_following": following,
        "git_public_repo": public_repo,
        "git_original_repo": orig_count,
        "git_authored_repo": authored_count,
        "last_commit_date": last_commit_date,
        "git_badges": _join_list(git_badges_list),
        "lc_total_solved": totalSolved,
        "lc_easy": easy,
        "lc_medium": medium,
        "lc_hard": hard,
        "lc_ranking": ranking,
        "lc_lastsubmission": lastsubmission,
    "lc_lastacceptedsubmission": lastacceptedsubmission,
    "lc_cur_streak": lc_cur_streak,
    "lc_max_streak": lc_max_streak,
    "lc_badges": str(lc_badges_count) if lc_badges_count is not None else None,
        "lc_language": _join_list(languages),
        # universal histories as JSON strings for portability
        "lc_submission_history": (None if lc_history is None else __import__("json").dumps(lc_history, separators=(",", ":"))),
        "gh_contribution_history": (None if gh_history is None else __import__("json").dumps(gh_history, separators=(",", ":"))),
    }


def update_all_students(engine: Engine, source_table: str, target_table: str) -> Tuple[int, List[str]]:
    inspector = inspect(engine)
    if not inspector.has_table(source_table):
        raise ValueError(f"Source table '{source_table}' does not exist")
    if not inspector.has_table(target_table):
        raise ValueError(f"Target table '{target_table}' does not exist")

    src = student_table(source_table)
    dst = data_table(target_table)
    notif_tbl = notification_table()

    updated = 0
    errors: List[str] = []
    # Ensure notification table exists (idempotent)
    try:
        create_notification_table(engine)
    except Exception:
        pass

    # Pull student list (outside long transaction)
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                src.c.roll_number,
                src.c.github_username,
                src.c.leetcode_username,
                src.c.name,
            )
        ).all()

    # Prepare work items
    work: List[Dict[str, Any]] = []
    for r in rows:
        roll = int(r.roll_number)
        gh = (r.github_username or '').strip()
        lc = (r.leetcode_username or '').strip()
        nm = (r.name or '').strip()
        if not gh and not lc:
            continue
        work.append({"roll": roll, "gh": gh, "lc": lc, "name": nm})

    if not work:
        return 0, []

    # Concurrency and batch settings (tunable via env)
    max_workers = int(os.getenv("STATS_MAX_WORKERS", "12"))  # slightly conservative default
    batch_size = int(os.getenv("DB_UPSERT_BATCH_SIZE", "30"))
    micro_batch_size = int(os.getenv("DB_MICRO_BATCH_SIZE", "8"))  # each VALUES group size
    max_retries = int(os.getenv("DB_MAX_RETRIES", "3"))
    base_sleep = float(os.getenv("DB_RETRY_BASE_SLEEP", "0.5"))
    batch_size = max(1, min(batch_size, 100))  # clamp
    micro_batch_size = max(1, min(micro_batch_size, batch_size))

    # Fetch all external data concurrently
    def _fetch_and_compute(item: Dict[str, Any]) -> Tuple[int, Dict[str, Any], str, Optional[str]]:
        roll = item["roll"]
        gh = item["gh"]
        lc = item["lc"]
        name = item["name"]
        git_json: Dict[str, Any] = {}
        lc_prof: Dict[str, Any] = {}
        lc_lang: Dict[str, Any] = {}
        lc_bdg: Dict[str, Any] = {}
        try:
            if gh:
                git_json = get_github_summary(gh)
                git_contri = get_github_contributions(gh)
            if lc:
                lc_prof = get_leetcode_profile(lc)
                lc_lang = get_leetcode_language_stats(lc)
                lc_bdg = get_leetcode_badges(lc)
                lc_calendar = get_leetcode_calendar(lc)
            stats = compute_stats(git_json, lc_prof, lc_lang, lc_bdg, git_contri, lc_calendar)
            return roll, stats, name, None
        except Exception as e:
            return roll, {}, name, f"roll={roll}: {type(e).__name__}: {e}"

    results: List[Tuple[int, Dict[str, Any], str]] = []
    to_add_notif: List[Dict[str, Any]] = []
    to_remove_notif: List[int] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {ex.submit(_fetch_and_compute, item): item for item in work}
        for fut in as_completed(fut_map):
            roll, stats, name, err = fut.result()
            if err:
                errors.append(err)
                continue
            results.append((roll, stats, name))
            # Notification decision now; we'll apply in bulk later
            lc_last = stats.get("lc_lastsubmission")
            reason_text = "No LC submission in last 3 days"
            flag = False
            if lc_last:
                try:
                    last_date = datetime.strptime(lc_last, "%Y-%m-%d").date()
                    today = datetime.now(tz=timezone.utc).date()
                    delta = (today - last_date).days
                    flag = delta > 3
                except Exception:
                    flag = True
            else:
                flag = True
            if flag:
                to_add_notif.append({
                    "table_name": source_table,
                    "rollnumber": roll,
                    "name": name,
                    "reason": reason_text,
                })
            else:
                to_remove_notif.append(roll)

    # Batch upsert into target table
    def _chunks(seq: List[Any], size: int):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]

    with engine.begin() as conn:
        # Snapshot destination column names to filter payloads (backward compatible)
        dst_cols = {c.name for c in dst.columns}
        # Helper to execute one micro batch with retries
        def _execute_payload(payload_list: List[Dict[str, Any]]):
            nonlocal updated
            if not payload_list:
                return
            stmt = pg_insert(dst).values(payload_list)
            update_cols = {k: stmt.excluded[k] for k in payload_list[0].keys() if k != "rollnumber"}
            stmt = stmt.on_conflict_do_update(
                index_elements=[dst.c.rollnumber],
                set_=update_cols,
            )
            attempt = 0
            while True:
                try:
                    conn.execute(stmt)
                    updated += len(payload_list)
                    return
                except (OperationalError, InterfaceError) as oe:
                    attempt += 1
                    if attempt > max_retries:
                        errors.append(f"upsert retries exceeded ({len(payload_list)} rows): {type(oe).__name__}: {oe}")
                        return
                    sleep_for = base_sleep * (2 ** (attempt - 1))
                    time.sleep(sleep_for)
                except Exception as e:
                    errors.append(f"upsert error ({len(payload_list)} rows): {type(e).__name__}: {e}")
                    return

        # Outer batches subdivided to micro-batches to limit single statement size
        for outer in _chunks(results, batch_size):
            for micro in _chunks(outer, micro_batch_size):
                raw = [{"rollnumber": roll, **stats} for (roll, stats, _name) in micro]
                # Filter to existing columns only
                payload = [{k: v for k, v in row.items() if k in dst_cols} for row in raw]
                _execute_payload(payload)

        # Apply notifications in batches
        # 1) remove stale reasons for non-stale users
        reason_text = "No LC submission in last 3 days"
        for roll_chunk in _chunks(to_remove_notif, max(1, batch_size)):
            try:
                if not roll_chunk:
                    continue
                del_stmt = (
                    delete(notif_tbl)
                    .where(
                        notif_tbl.c.table_name == source_table,
                        notif_tbl.c.reason == reason_text,
                        notif_tbl.c.rollnumber.in_(roll_chunk),
                    )
                )
                conn.execute(del_stmt)
            except Exception as ne:
                errors.append(f"notif-remove batch: {type(ne).__name__}: {ne}")

        # 2) upsert notifications to add
        for chunk in _chunks(to_add_notif, batch_size):
            for micro in _chunks(chunk, micro_batch_size):
                if not micro:
                    continue
                stmt = pg_insert(notif_tbl).values(micro)
                stmt = stmt.on_conflict_do_update(
                    index_elements=[notif_tbl.c.table_name, notif_tbl.c.rollnumber],
                    set_={
                        "name": stmt.excluded.name,
                        "reason": stmt.excluded.reason,
                    },
                )
                attempt = 0
                while True:
                    try:
                        conn.execute(stmt)
                        break
                    except (OperationalError, InterfaceError) as oe:
                        attempt += 1
                        if attempt > max_retries:
                            errors.append(f"notif-upsert retries exceeded: {type(oe).__name__}: {oe}")
                            break
                        time.sleep(base_sleep * (2 ** (attempt - 1)))
                    except Exception as ne:
                        errors.append(f"notif-upsert batch: {type(ne).__name__}: {ne}")
                        break

    return updated, errors
