from flask import Flask, request, jsonify
import duckdb
import logging
import pandas as pd
import os
from datetime import datetime, timedelta  # <-- Add timedelta here
import calendar
from typing import List, Dict
import pytz
from dateutil.relativedelta import relativedelta
 
 
app = Flask(__name__)
 
# Configure logging
logging.basicConfig(filename='api_logs.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
 
# Connect to DuckDB (in-memory for simplicity)
con = duckdb.connect(database=':memory:')
 
# Create items table
con.execute("""
CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    value INTEGER NOT NULL
)
""")
 
# Create API logs table
con.execute("""
CREATE TABLE IF NOT EXISTS api_logs (
    id INTEGER PRIMARY KEY,
    level VARCHAR,
    api_type VARCHAR,
    response_status VARCHAR,
    message VARCHAR,
    endpoint VARCHAR,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    time_diff VARCHAR
)
""")
 
EXCEL_FILE = 'logs.xlsx'
FORMAT_FILE_1 = 'format_file1.xlsx'
FORMAT_FILE_2 = 'format_file2.xlsx'
 
# Function to log request info in database and export to Excel
def log_request_info(api_type, response_status, message, endpoint, start_time, end_time):
    time_diff = (end_time - start_time).total_seconds()  # Calculate time difference in seconds
 
    # Compute next available ID for the api_logs table
    next_id = con.execute("SELECT IFNULL(MAX(id), 0) + 1 FROM api_logs").fetchone()[0]
 
    log_entry = {
        'id': next_id,
        'level': 'Info',
        'api_type': api_type,
        'response_status': response_status,
        'message': message,
        'endpoint': endpoint,
        'timestamp': end_time.strftime('%Y-%m-%d %H:%M:%S'),
        'time_diff': f"{time_diff:.4f}"
    }
 
    # Insert the log entry into the database
    con.execute(
        "INSERT INTO api_logs (id, level, api_type, response_status, message, endpoint, timestamp, time_diff) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (log_entry['id'], log_entry['level'], log_entry['api_type'], log_entry['response_status'],
         log_entry['message'], log_entry['endpoint'], log_entry['timestamp'], log_entry['time_diff'])
    )
 
    # Log to the file
    logging.info(f"API Log: {log_entry}")
 
    # Export logs to Excel
    export_logs_to_excel()
 
# Function to export logs to Excel
def export_logs_to_excel():
    # Fetch all logs from the api_logs table
    new_logs = con.execute("SELECT * FROM api_logs").fetchdf()
 
    # Check if the Excel file already exists
    if os.path.exists(EXCEL_FILE):
        # Load the existing data into a DataFrame
        existing_logs = pd.read_excel(EXCEL_FILE)
 
        # Concatenate the new logs with the existing ones
        combined_logs = pd.concat([existing_logs, new_logs], ignore_index=True)
    else:
        # If the file doesn't exist, use the new logs as the combined logs
        combined_logs = new_logs
 
    # Write the combined logs to Excel (in append mode)
    combined_logs.to_excel(EXCEL_FILE, index=False, engine='openpyxl')
 
    # Check if we should generate the new Excel reports
    generate_excel_reports()
 
# Function to generate new Excel reports based on log data
def generate_excel_reports():
    # Check if the Excel log file exists
    if not os.path.exists(EXCEL_FILE):
        print(f"{EXCEL_FILE} does not exist.")
        return
   
    # Read the Excel file into a DataFrame
    df = pd.read_excel(EXCEL_FILE)
 
    # Check if there are at least 2 new rows of logs
    if len(df) < 2:
        print("Not enough data to generate reports.")
        return
 
    # Generate format 1: Summary of request types over time
    generate_format_file1(df)
 
    # Generate format 2: Average response times for each endpoint
    generate_format_file2(df)
 
 
 
# Define your file path (you can change this to your specific file path)
FORMAT_FILE_1 = 'api_count.xlsx'
 
def generate_format_file1(df):
    # Assuming 'endpoint' and 'timestamp' columns exist in the logs
    df['timestamp'] = pd.to_datetime(df['timestamp'])
 
    # Floor timestamps to the nearest minute for grouping by minute
    df['minute_group'] = df['timestamp'].dt.floor('T')
 
    # Create a complete list of minute intervals between the first and last timestamp
    all_minutes = pd.date_range(df['minute_group'].min(), df['minute_group'].max(), freq='T')
 
    # Initialize an empty list to store summary data for each minute
    summary_data = []
 
    # Loop over each minute interval and calculate the API request statistics
    for minute in all_minutes:
        # Set the start and end time for the minute interval
        start_time = minute
        end_time = minute + pd.Timedelta(minutes=1)
 
        # Filter data for the current minute
        df_minute = df[(df['timestamp'] >= start_time) & (df['timestamp'] < end_time)]
 
        # Count requests for each endpoint and method in the current minute
        total_requests = len(df_minute)
        post_requests = len(df_minute[(df_minute['api_type'] == 'POST') & (df_minute['endpoint'] == '/items')])
        get_requests = len(df_minute[(df_minute['api_type'] == 'GET') & (df_minute['endpoint'] == '/items')])
        delete_requests = len(df_minute[(df_minute['api_type'] == 'DELETE') & (df_minute['endpoint'].str.contains('/items'))])
        put_requests = len(df_minute[(df_minute['api_type'] == 'PUT') & (df_minute['endpoint'] == '/items')])
        get_api_logs_requests = len(df_minute[(df_minute['api_type'] == 'GET') & (df_minute['endpoint'] == '/api_logs')])
 
        # Create the timerange string in the desired format: 'start_time - end_time'
        timerange = f"{start_time.strftime('%Y-%m-%d : %H:%M:%S')} - {end_time.strftime('%Y-%m-%d : %H:%M:%S')}"
 
        # Append the summary data for the current minute
        summary_data.append({
            'timerange': timerange,
            'total': total_requests,
            '/items post': post_requests,
            '/items get': get_requests,
            'items/put': put_requests,
            '/delete': delete_requests,
            '/api_logs/get': get_api_logs_requests
        })
 
    # Create a DataFrame from the summary data
    df_format1 = pd.DataFrame(summary_data)
 
    # Check if the file already exists, append new data if it does
    if os.path.exists(FORMAT_FILE_1):
        # Read existing data from Excel
        existing_data = pd.read_excel(FORMAT_FILE_1)
 
        # Ensure consistent column structure and avoid misalignment
        existing_data = existing_data.reindex(columns=df_format1.columns, fill_value="")
 
        # Concatenate the existing data with the new data
        combined_data = pd.concat([existing_data, df_format1], ignore_index=True)
 
        # Drop duplicate rows based on the 'timerange' column (optional)
        combined_data = combined_data.drop_duplicates(subset=['timerange'], keep='last')
 
        # Save the updated data back to the Excel file
        combined_data.to_excel(FORMAT_FILE_1, index=False, engine='openpyxl')
    else:
        # If the file does not exist, create a new file with the new data
        df_format1.to_excel(FORMAT_FILE_1, index=False, engine='openpyxl')
 
 
# Example usage
if __name__ == '__main__':
    # Example log data (replace this with your actual log data)
    data = {
        'timestamp': ['2024-10-18 10:01:01', '2024-10-18 10:01:59', '2024-10-18 10:02:15', '2024-10-18 10:03:45'],
        'api_type': ['POST', 'GET', 'DELETE', 'GET'],
        'endpoint': ['/items', '/items', '/items/123', '/api_logs']
    }
   
    # Create a DataFrame
    df = pd.DataFrame(data)
 
    # Call the function
    generate_format_file1(df)
 
 
 
 
 
# Define your file path (you can change this to your specific file path)
FORMAT_FILE_2 = 'response_time.xlsx'
 
def generate_format_file2(df):
    # Convert 'timestamp' column to datetime if it's not already in the correct format
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
 
    # Convert time_diff to float and multiply by 1000 to convert seconds to milliseconds
    df['time_diff'] = df['time_diff'].astype(float) * 1000  # Convert to milliseconds
 
    # Floor timestamps to the nearest minute for grouping by minute
    df['minute_group'] = df['timestamp'].dt.floor('T')
 
    # Create a complete list of minute intervals between the first and last timestamp
    all_minutes = pd.date_range(df['minute_group'].min(), df['minute_group'].max(), freq='T')
 
    # Initialize an empty list to store summary data for each minute
    summary_data = []
 
    # Loop over each minute interval and calculate the average response times
    for minute in all_minutes:
        # Set the start and end time for the minute interval
        start_time = minute
        end_time = minute + pd.Timedelta(minutes=1)
 
        # Filter data for the current minute
        df_minute = df[(df['timestamp'] >= start_time) & (df['timestamp'] < end_time)]
 
        if not df_minute.empty:
            # Calculate average response times for each endpoint in the current minute
            avg_response_time = df_minute['time_diff'].mean()
            avg_post_time = df_minute[(df_minute['api_type'] == 'POST') & (df_minute['endpoint'] == '/items')]['time_diff'].mean()
            avg_get_time = df_minute[(df_minute['api_type'] == 'GET') & (df_minute['endpoint'] == '/items')]['time_diff'].mean()
 
            # Create the timerange string in the desired format: 'start_time - end_time'
            timerange = f"{start_time.strftime('%Y-%m-%d : %H:%M:%S')} - {end_time.strftime('%Y-%m-%d : %H:%M:%S')}"
 
            # Append the summary data for the current minute
            summary_data.append({
                'timerange': timerange,
                'avg_response_time': avg_response_time,
                'avg_response_endpoint_item_post': avg_post_time,
                'avg_response_endpoint_item_get': avg_get_time
            })
 
    # Create a DataFrame from the summary data
    df_format2 = pd.DataFrame(summary_data)
 
    # Write to Excel
    if os.path.exists(FORMAT_FILE_2):
        # Read existing data from Excel
        existing_data = pd.read_excel(FORMAT_FILE_2)
 
        # Ensure consistent column structure and avoid misalignment
        existing_data = existing_data.reindex(columns=df_format2.columns, fill_value="")
 
        # Concatenate the existing data with the new data
        combined_data = pd.concat([existing_data, df_format2], ignore_index=True)
 
        # Drop duplicate rows based on the 'timerange' column (optional)
        combined_data = combined_data.drop_duplicates(subset=['timerange'], keep='last')
 
        # Save the updated data back to the Excel file
        combined_data.to_excel(FORMAT_FILE_2, index=False, engine='openpyxl')
    else:
        # If the file does not exist, create a new file with the new data
        df_format2.to_excel(FORMAT_FILE_2, index=False, engine='openpyxl')
 
# Example usage
if __name__ == '__main__':
    # Example log data (replace this with your actual log data)
    data = {
        'timestamp': ['2024-10-18 10:01:01', '2024-10-18 10:01:59', '2024-10-18 10:02:15', '2024-10-18 10:03:45'],
        'api_type': ['POST', 'GET', 'DELETE', 'GET'],
        'endpoint': ['/items', '/items', '/items/123', '/api_logs'],
        'time_diff': [0.009, 0.008, 0.01, 0.007]  # in seconds
    }
   
    # Create a DataFrame
    df = pd.DataFrame(data)
 
    # Call the function
    generate_format_file2(df)
 
 
@app.route('/data/api_count', methods=['GET'])
def get_api_count():
    try:
        df = pd.read_excel('api_count.xlsx')  # Read Excel file into DataFrame
        data = df.to_dict(orient='records')  # Convert DataFrame to list of dictionaries
        return jsonify(data)  # Return data as JSON
    except Exception as e:
        return jsonify({'error': str(e)}), 500
   
@app.route('/data/response_time', methods=['GET'])
def get_response_time():
    try:
        df = pd.read_excel('response_time.xlsx')  # Read Excel file into DataFrame
        data = df.to_dict(orient='records')  # Convert DataFrame to list of dictionaries
        return jsonify(data)  # Return data as JSON
    except Exception as e:
        return jsonify({'error': str(e)}), 500
 
 
try:
    df = pd.read_excel(FORMAT_FILE_1)
except Exception as e:
    print(f"Error loading Excel file: {e}")
 
# Parse timerange into start and end times
def parse_datetime_range(timerange):
    try:
        start_str, end_str = timerange.split(' - ')
        start_time = pd.to_datetime(start_str.strip(), format='%Y-%m-%d : %H:%M:%S')
        end_time = pd.to_datetime(end_str.strip(), format='%Y-%m-%d : %H:%M:%S')
        return start_time, end_time
    except Exception as e:
        raise ValueError(f"Invalid timerange format: {timerange}. Error: {e}")
 
# Generate expected intervals
def generate_empty_intervals(start, end, freq):
    return pd.date_range(start=start, end=end, freq=freq).to_frame(index=False, name='start_time')
 
# Aggregate data across specified intervals
def aggregate_data(df, start, end, freq, expected_intervals):
    df[['start_time', 'end_time']] = df['timerange'].apply(lambda x: pd.Series(parse_datetime_range(x)))
 
    # Ensure filtered data is within the requested range
    mask = (df['start_time'] >= start) & (df['start_time'] < end)
    filtered_df = df[mask]
 
    # Resample the data by the requested frequency
    resampled = (
        filtered_df.set_index('start_time')
        .resample(freq, closed='left', label='left')
        .sum(numeric_only=True)
        .reset_index()
    )
 
    # Merge with expected intervals to fill missing periods with 0
    merged_df = pd.merge(expected_intervals, resampled, on='start_time', how='left').fillna(0)
 
    result = []
    for _, row in merged_df.iterrows():
        result.append({
            'time': row['start_time'].strftime('%Y-%m-%d %H:%M:%S'),
            'total': int(row.get('total', 0)),
            '/items post': int(row.get('/items post', 0)),
            '/items get': int(row.get('/items get', 0)),
            'items/put': int(row.get('items/put', 0)),
            '/delete': int(row.get('/delete', 0)),
            '/api_logs/get': int(row.get('/api_logs/get', 0))
        })
    return result
 
@app.route('/api/aggregate', methods=['GET'])
def aggregate_api_counts():
    try:
        now = datetime.now().replace(second=0, microsecond=0)
        year = request.args.get('year')
        month = request.args.get('month')
        week = request.args.get('week')
        day = request.args.get('day')
        hour = request.args.get('hour')
 
        if year is not None:
            start = now - timedelta(days=365)
            expected_intervals = generate_empty_intervals(start, now, 'M')
            result = aggregate_data(df, start, now, 'M', expected_intervals)
 
        elif month is not None:
            start = now.replace(day=1) - timedelta(days=30)
            expected_intervals = generate_empty_intervals(start, now, 'D')
            result = aggregate_data(df, start, now, 'D', expected_intervals)
 
        elif week is not None:
            start = now - timedelta(weeks=4)
            expected_intervals = generate_empty_intervals(start, now, 'W-MON')
            result = aggregate_data(df, start, now, 'W-MON', expected_intervals)
 
        elif day is not None:
            start = now - timedelta(days=1)
            expected_intervals = generate_empty_intervals(start, now, 'H')
            result = aggregate_data(df, start, now, 'H', expected_intervals)
 
        elif hour is not None:
            start = now - timedelta(hours=1)
            expected_intervals = generate_empty_intervals(start, now, 'T')
            result = aggregate_data(df, start, now, 'T', expected_intervals)
 
        else:
            return jsonify({'error': 'Invalid query parameters'}), 400
 
        return jsonify(result)
 
    except KeyError as e:
        return jsonify({'error': f"Missing column: {e}"}), 500
    except ValueError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500
 
# Route to fetch API logs
@app.route('/api_logs', methods=['GET'])
def get_api_logs():
    try:
        logs = con.execute("SELECT * FROM api_logs").fetchall()
        log_list = [
            {
                "level": log[1],
                "api_type": log[2],
                "response_status": log[3],
                "message": log[4],
                "endpoint": log[5],
                "timestamp": log[6].strftime('%Y-%m-%d %H:%M:%S'),
                "time_diff": log[7]
            }
            for log in logs
        ]
        return jsonify(log_list), 200
    except Exception as e:
        logging.error(f"Error retrieving logs: {e}")
        return jsonify({'error': 'Could not retrieve logs'}), 500
 
# Route to add a new item
@app.route('/items', methods=['POST'])
def add_item():
    start_time = datetime.now()
   
    try:
        # Log incoming request data
        data = request.json
        logging.info(f"Received data: {data}")
       
        # Extract fields from the request body
        name = data.get('name')
        value = data.get('value')
 
        # Validate that both 'name' and 'value' are present
        if not name or value is None:
            raise ValueError("Both 'name' and 'value' fields are required.")
 
        # Execute the SQL insert query
        con.execute(
            "INSERT INTO items (id, name, value) VALUES ((SELECT IFNULL(MAX(id), 0) + 1 FROM items), ?, ?)",
            (name, value)
        )
       
        # Prepare the success response
        response = jsonify({"message": "Item added successfully!"})
        response.status_code = 201
 
    except ValueError as ve:
        # Handle missing fields
        logging.error(f"Value error: {ve}")
        response = jsonify({"error": str(ve)})
        response.status_code = 400
 
    except Exception as e:
        logging.error(f"Error adding item: {e}")
        response = jsonify({"error": "An error occurred while adding the item."})
        response.status_code = 500
 
    end_time = datetime.now()
   
    # Log the request info
    log_request_info('POST', response.status_code, response.get_json().get('message', ''), '/items', start_time, end_time)
 
    return response
 
# Route to get all items
@app.route('/items', methods=['GET'])
def get_items():
    start_time = datetime.now()
    try:
        items = con.execute("SELECT * FROM items").fetchall()
       
        # Log the fetched items for debugging
        logging.info(f"Fetched items: {items}")
       
        # Ensure items is not empty
        if not items:
            return jsonify([]), 200  # Return empty list if no items
 
        response = jsonify([{"id": item[0], "name": item[1], "value": item[2]} for item in items])
        response.status_code = 200
    except Exception as e:
        logging.error(f"Error fetching items: {e}")
        response = jsonify({'error': 'Could not retrieve items'})
        response.status_code = 500
 
    end_time = datetime.now()
    log_request_info('GET', str(response.status_code), 'Items fetched', '/items', start_time, end_time)
 
    return response
 
 
# Route to update an item
@app.route('/items/<int:item_id>', methods=['PUT'])
def update_item(item_id):
    start_time = datetime.now()
   
    try:
        # Log incoming request data
        data = request.json
        logging.info(f"Received data for item ID {item_id}: {data}")
       
        # Extract fields from the request body
        name = data.get('name')
        value = data.get('value')
 
        # Validate that at least one field is present
        if name is None and value is None:
            raise ValueError("At least one of 'name' or 'value' fields must be provided.")
 
        # Construct the SQL update query
        set_clause = []
        params = []
 
        if name is not None:
            set_clause.append("name = ?")
            params.append(name)
        if value is not None:
            set_clause.append("value = ?")
            params.append(value)
 
        params.append(item_id)  # Add item_id for the WHERE clause
        query = f"UPDATE items SET {', '.join(set_clause)} WHERE id = ?"
       
        # Execute the SQL update query
        con.execute(query, params)
       
        # Prepare the success response
        response = jsonify({"message": "Item updated successfully!"})
        response.status_code = 200
 
    except ValueError as ve:
        # Handle missing fields
        logging.error(f"Value error: {ve}")
        response = jsonify({"error": str(ve)})
        response.status_code = 400
 
    except Exception as e:
        logging.error(f"Error updating item: {e}")
        response = jsonify({"error": "An error occurred while updating the item."})
        response.status_code = 500
 
    end_time = datetime.now()
   
    # Log the request info
    log_request_info('PUT', response.status_code, response.get_json().get('message', ''), f'/items/{item_id}', start_time, end_time)
 
    return response
 
# Route to delete an item
@app.route('/items/<int:item_id>', methods=['DELETE'])
def delete_item(item_id):
    start_time = datetime.now()
   
    try:
        # Execute the SQL delete query
        con.execute("DELETE FROM items WHERE id = ?", (item_id,))
        response = jsonify({"message": "Item deleted successfully!"})
        response.status_code = 200
    except Exception as e:
        logging.error(f"Error deleting item: {e}")
        response = jsonify({"error": "An error occurred while deleting the item."})
        response.status_code = 500
 
    end_time = datetime.now()
 
    # Log the request info
    log_request_info('DELETE', response.status_code, response.get_json().get('message', ''), f'/items/{item_id}', start_time, end_time)
 
    return response
 

 
 
def load_excel_data():
    """Load data from Excel file and convert timerange to datetime"""
    if not os.path.exists('api_count.xlsx'):
        return pd.DataFrame()
    df = pd.read_excel('api_count.xlsx')
    def parse_timerange(timerange):
        start_time_str = timerange.split(' - ')[0]
        start_time_str = start_time_str.replace(' : ', ' ')
        dt = pd.to_datetime(start_time_str)
        return dt.tz_localize('UTC')
    df['start_time'] = df['timerange'].apply(parse_timerange)
    return df
 
def create_empty_metrics():
    """Create empty metrics dictionary"""
    return {
        'total_requests': 0,
        'post_requests': 0,
        'get_requests': 0,
        'put_requests': 0,
        'delete_requests': 0,
        'api_logs_requests': 0
    }
 
def aggregate_data(df, start_time, end_time, freq, time_filter):
    """Aggregate data based on time range and frequency"""
    if df.empty:
        df = pd.DataFrame(columns=['start_time', 'total', '/items post', '/items get', 
                                 'items/put', '/delete', '/api_logs/get'])
    
    # Ensure start_time and end_time are timezone-aware
    if start_time.tzinfo is None:
        start_time = pytz.UTC.localize(start_time)
    if end_time.tzinfo is None:
        end_time = pytz.UTC.localize(end_time)
    
    # Convert timestamps to pandas datetime
    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)
    
    # Create a complete date range based on the filter
    if time_filter == 'year':
        # Create dates for each month on the same day of the month
        dates = []
        current_date = start_time
        while current_date <= end_time:
            dates.append(current_date)
            # Use relativedelta to increment by exactly one month while keeping the same day
            current_date = current_date + relativedelta(months=1)
        date_range = pd.DatetimeIndex(dates)
    elif time_filter == 'week':
        # Create 7 daily periods for the week
        date_range = pd.date_range(start=start_time, periods=7, freq='D')
    elif time_filter == 'month':
        # Create daily periods for the entire month
        date_range = pd.date_range(start=start_time, end=end_time, freq='D')
    elif time_filter == 'day':
        # Create exactly 24 hourly periods
        date_range = pd.date_range(start=start_time, periods=24, freq='H')
    else:  # hour
        # Create exactly 60 minute periods
        date_range = pd.date_range(start=start_time, periods=60, freq='T')
    
    # Filter data within the time range
    mask = (df['start_time'] >= start_time) & (df['start_time'] < end_time)
    filtered_df = df[mask]
    
    # Create a DataFrame with all periods
    complete_df = pd.DataFrame(index=date_range)
    complete_df.index.name = 'start_time'
    
    if not filtered_df.empty:
        # Group by time frequency and aggregate
        grouped = filtered_df.set_index('start_time').resample(freq).agg({
            'total': 'sum',
            '/items post': 'sum',
            '/items get': 'sum',
            'items/put': 'sum',
            '/delete': 'sum',
            '/api_logs/get': 'sum'
        })
        # Merge with complete range
        complete_df = complete_df.join(grouped).fillna(0)
    else:
        # Add empty columns if no data
        for col in ['total', '/items post', '/items get', 'items/put', '/delete', '/api_logs/get']:
            complete_df[col] = 0
    
    # Convert to list of dictionaries with consistent structure
    result = []
    for timestamp in date_range:
        if timestamp in complete_df.index:
            row = complete_df.loc[timestamp]
            result.append({
                'time_period': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'metrics': {
                    'total_requests': int(row.get('total', 0)),
                    'post_requests': int(row.get('/items post', 0)),
                    'get_requests': int(row.get('/items get', 0)),
                    'put_requests': int(row.get('items/put', 0)),
                    'delete_requests': int(row.get('/delete', 0)),
                    'api_logs_requests': int(row.get('/api_logs/get', 0))
                }
            })
        else:
            result.append({
                'time_period': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'metrics': create_empty_metrics()
            })
    return result

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    try:
        time_filter = request.args.get('filter', 'hour')
        df = load_excel_data()
        now = datetime.now(pytz.UTC)
        
        if time_filter == 'year':
            # Calculate exactly one year ago from current date
            start_time = now - relativedelta(years=1)
            freq = 'M'
            response_key = 'monthly_metrics'
        elif time_filter == 'week':
            # Calculate exactly one week ago
            start_time = now - timedelta(days=7)
            freq = 'D'
            response_key = 'daily_metrics'
        elif time_filter == 'month':
            start_time = now - timedelta(days=30)
            freq = 'D'
            response_key = 'daily_metrics'
        elif time_filter == 'day':
            start_time = now - timedelta(days=1)
            freq = 'H'
            response_key = 'hourly_metrics'
        elif time_filter == 'hour':
            start_time = now - timedelta(hours=1)
            freq = 'T'
            response_key = 'minute_metrics'
        else:
            return jsonify({'error': 'Invalid time filter. Use: year, month, week, day, or hour'}), 400
            
        aggregated_data = aggregate_data(df, start_time, now, freq, time_filter)
        
        # Verify the number of objects matches the requirements
        expected_counts = {
            'year': 12,    # 12 months
            'week': 7,     # 7 days
            'month': (now - start_time).days + 1,  # number of days in the period
            'day': 24,     # 24 hours
            'hour': 60     # 60 minutes
        }
        
        if len(aggregated_data) != expected_counts[time_filter]:
            print(f"Warning: Expected {expected_counts[time_filter]} objects for {time_filter} filter, got {len(aggregated_data)}")
            
        response = {
            'time_filter': time_filter,
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': now.strftime('%Y-%m-%d %H:%M:%S'),
            'count': len(aggregated_data),
            response_key: aggregated_data
        }
        return jsonify(response)
    except Exception as e:
        import traceback
        error_details = {
            'error': f'An error occurred: {str(e)}',
            'traceback': traceback.format_exc()
        }
        return jsonify(error_details), 500
 
if __name__ == '__main__':
    app.run(debug=True, port=5000)
