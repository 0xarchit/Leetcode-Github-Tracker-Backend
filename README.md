# LeetCode & GitHub Tracker Backend

This project is a FastAPI-based backend designed to track and manage student data from LeetCode and GitHub. It provides a multi-tenant system where student information can be organized into separate tables, with corresponding data tables for storing fetched statistics. The application fetches data from external APIs, processes it, and stores it in a PostgreSQL database.

## Features

- **Multi-tenant**: Create and manage separate tables for different groups of students.
- **Data Aggregation**: Fetches and aggregates student data from LeetCode and GitHub, including contribution history, problem stats, and repository information.
- **Database Management**: Automatically creates and manages necessary tables in a PostgreSQL database.
- **API Security**: Endpoints are protected with a password.
- **Notifications**: A system to flag students based on configurable criteria (e.g., inactivity).
- **CORS Enabled**: Allows requests from any origin, making it easy to integrate with a frontend application.

## Tech Stack

- **Backend**: FastAPI
- **Database**: PostgreSQL (with SQLAlchemy and psycopg2)
- **Server**: Uvicorn
- **Dependencies**: See `requirements.txt`

## Setup and Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Create and activate a virtual environment:**
    ```powershell
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Set up environment variables:**
    Create a `.env` file in the root of the project and add the following variables:
    ```env
    POSTGRES_CONNECT_STRING="postgresql://user:password@host:port/dbname"
    PASSWORD="your-api-password"
    GITHUB_API="your-github-api-endpoint"
    LEETCODE_API="your-leetcode-api-endpoint"
    ```

## Running the Application

To run the application locally, use the following command from the project root:

```powershell
$env:PYTHONPATH = (Get-Location).Path
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

The server will be available at `http://localhost:8000`.

## API Endpoints

All endpoints require a `password` query parameter for authentication.

### Health

- **GET `/health`**: Checks the database connection.

### Table Management

- **POST `/addtable`**: Creates a new table for students.
  - **Body**: `{"table_name": "your_table_name"}`
- **POST `/addDataTable`**: Creates a data table for the corresponding student table.
  - **Body**: `{"table_name": "your_table_name"}`
- **GET `/available`**: Lists all available student tables.

### Student Management

- **POST `/add`**: Adds or updates a student's information in a specified table.
  - **Body**:
    ```json
    {
      "table_name": "your_table_name",
      "name": "Student Name",
      "roll_number": 123,
      "github_username": "student-gh",
      "leetcode_username": "student-lc"
    }
    ```

### Data Operations

- **POST `/update`**: Fetches the latest data from GitHub and LeetCode for all students in a table and updates the database.
  - **Body**: `{"table_name": "your_table_name"}`
- **POST `/data`**: Retrieves the combined student and tracking data for a table.
  - **Body**: `{"table_name": "your_table_name"}`
- **GET `/lastUpdate`**: Shows the timestamp of the last update for each table.

### Notifications

- **POST `/addNotif`**: Adds a notification for a student.
  - **Body**: `{"table_name": "your_table_name", "roll_number": 123, "reason": "Inactive"}`
- **POST `/removeNotif`**: Removes a notification for a student.
  - **Body**: `{"table_name": "your_table_name", "roll_number": 123}`
- **GET `/showNotif`**: Lists all active notifications.

## Database Schema

The application uses two main types of tables:

1.  **Student Tables**: Named by the user (e.g., `btech_2025`). Stores basic student information like name, roll number, and usernames.
2.  **Data Tables**: Automatically named with a `_Data` suffix (e.g., `btech_2025_Data`). Stores the fetched data from LeetCode and GitHub, including contribution history as JSONB.

The application will automatically create these tables if they don't exist.
