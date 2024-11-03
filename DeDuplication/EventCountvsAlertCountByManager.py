# ==========================================================================================#
# File: EventCountvsAlertCountByManager.py                                                                 #
# Description: Fetches and aggregates event counts by manager from                          #
#              Dell APEX AIOps IM/Moogsoft API. The API pulls data in batches.              #
#              The results are aggregrated to present events counts per ingress             #
#              source(managers) for the last 3 months and current MTD.                      #
#               We rely on the event_count for the respective alers to get event details.   #
#                                                                                           #
# Author: Bala Baskaran                                                                     #
# Created: 2024-10-24                                                                       #
# Version: 1.0.0                                                                            #
# ==========================================================================================#

# Imports for the Python Script
import requests
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import time

# Constants for the POST API call

API_KEY = 'PROVIDE YOUR APIKEY'
URL = "https://api.moogsoft.ai/v1/alerts"
HEADERS = {
    'Content-Type': 'application/json',
    'apiKey': API_KEY
}

# Function to make a POST request and return the response data
# The json filter can be built using the AG-Grid-style JSON filter from UI
def fetch_moog_alerts(date_from, date_to, search_after):
    payload = json.dumps({
        "json_filter": {
            "created_at": {
                "dateFrom": date_from,
                "dateTo": date_to,
                "filterType": "date",
                "type": "inRange"
            }
        },
        "json_sort": [
            {"sort": "asc", "colId": "alert_id"}
        ],
        "utc_offset": "GMT-0",
        #"start": 0,
        "limit": 4000,
        "fields": ["manager", "alert_id", "event_count"],
        #search_after allows to scroll number of alerts for consolidation
        "search_after": search_after
    })

    #Ready with the payload, lets make a call to the endpoint and fetch data
    response = requests.post(URL, headers=HEADERS, data=payload)
    
    # Print the raw JSON response after each API call - More of a debug 
    print(f"Response for {date_from} to {date_to}:")
    # Pretty print the JSON
    print(json.dumps(response.json(), indent=4))  
    return response.json()

# Function to iterate through the 3 month data in 4 hour intervals
def fetch_events_from_alerts():
    # Get the current date and calculate the start date ( Begin with 3 months ago)
    # Lets use UTC datetime for the date filters
    now = datetime.now(timezone.utc) 
    start_date = (now.replace(day=1) - timedelta(days=90)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    #manager_event_count = defaultdict(int)  # To store sum of event counts per manager
    #manager_event_count = defaultdict(lambda: defaultdict(int))  # Nested defaultdict to store per-month event counts
    manager_event_count = defaultdict(lambda: defaultdict(lambda: {'event_count': 0, 'alert_count': 0}))


    search_after = None  # Initialize search_after for first run
    
    current_time = start_date

    while current_time <  datetime.now(timezone.utc):
        next_window = current_time + timedelta(hours=4)
        date_from = current_time.strftime('%Y-%m-%d %H:%M:%S')
        date_to = next_window.strftime('%Y-%m-%d %H:%M:%S')
        results = []

        # Fetch alerts for the given time range and search_after value
        data = fetch_moog_alerts(date_from, date_to, search_after)
        

        results = data.get('data', {}).get('result', [])
        new_search_after = data.get('data', {}).get('search_after')

        month = current_time.strftime('%Y-%m')  # e.g., '2024-07'

        # Process each alert and accumulate event counts by manager
        for alert in results:
            manager = alert.get('manager', 'Unknown')
            event_count = alert.get('event_count', 0)
            manager_event_count[month][manager]['event_count'] += event_count
            manager_event_count[month][manager]['alert_count'] += 1

   
        search_after = new_search_after  # Use new search_after for next call
        print(f"\nSearching after Alert ID...: {search_after}")
        time.sleep(0.5)

        # Move to the next 4-hour window
        current_time = next_window
        
   # Print the final report for each month
    print("Manager level monthly event counts:")
    #tabular_formatted_data = []
    for month, managers in manager_event_count.items():
        print(f"\nMonth: {month}")
        if managers:
            # Sort managers by event count in descending order
            sorted_managers = sorted(managers.items(), key=lambda x: x[1]['event_count'], reverse=True)
            for manager, total_count in sorted_managers:
                print(f"  Manager: {manager}, Total Alerts: {total_count['alert_count']}, Total Events: {total_count['event_count']}")
                #tabular_formatted_data.append([manager,total_count])
        else:
            print("  No events recorded for this month.")

# Run the report generation
fetch_events_from_alerts()

