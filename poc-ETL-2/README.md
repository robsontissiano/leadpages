# Animal Data ETL

This project is an ETL (Extract, Transform, Load) system for fetching, transforming, and loading animal data from an API. The system fetches animal data from paginated endpoints, transforms certain fields, and then loads the data in batches to another API endpoint.

## Table of Contents
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Error Handling](#error-handling)
- [Logging](#logging)

## Requirements

This project requires Python 3.7 or above. The following Python packages are also required:

- `httpx`: For making HTTP requests asynchronously.
- `asyncio`: For handling asynchronous operations.
- `pydantic`: For validating and structuring API data.
- `python-dotenv`: To load environment variables (optional).

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/animal-data-etl.git
   cd animal-data-etl

2. Set up a virtual environment (recommended):
    ```
    python3 -m venv venv
    ```
source venv/bin/activate   # For macOS/Linux
or
.\venv\Scripts\activate    # For Windows


Install dependencies:
    ```
    pip install -r requirements.txt
    ```


## Configuration
1. Environment Variables:

Optionally, you can create a .env file to store configuration settings (like enabling verification) and ensure it's loaded automatically. For example:

    ```
    VERIFY=1
    ```

2. Update URLs:

By default, the script assumes the API is hosted locally at http://localhost:3123/animals/v1.

If your endpoints are hosted elsewhere, update the BASE_URL constant in the main.py file accordingly:

    ```
    BASE_URL = 'http://your-api-host-url/animals/v1'
    ```

# Usage
To run the ETL program, use the following command:

```
python main.py
```

The program will:

1. Fetch all paginated animal data.
2. Fetch detailed data for each animal.
3. Transform the friends field into a list and convert the born_at field (if present) into an ISO8601 UTC format.
4. Post animals in batches of up to 100 animals to the /home endpoint.

Example of Running the Program:
    ```
    python main.py
    ```

## Error Handling
* The server may randomly pause for 5-15 seconds or return HTTP 500, 502, 503, or 504 errors.
* The script will retry failed requests up to 5 times with randomized exponential backoff.
* If the issue persists beyond the retries, the error will be logged and the script will continue processing the next batch.

## Logging
Logs are saved to animal_loader.log in the root directory. The logs will contain information about:

* Successfully fetched and posted batches.
* Any retries due to server errors.
* Any unhandled exceptions during execution.

Sample log entry:
``` arduino
2024-10-08 14:32:45 - INFO - Fetching animals on page 1
2024-10-08 14:32:47 - INFO - Successfully posted batch: {'message': 'Helped 100 find home'}
2024-10-08 14:33:15 - WARNING - Server error (HTTPStatusError: 500 Server Error). Retrying in 7 seconds (Attempt 1/5)
```
