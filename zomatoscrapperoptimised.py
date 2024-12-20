import requests
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Google Sheets setup
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# IDs for the sheets
controller_sheet_id = "1mff16_JEma1sypVjtYPk6YVXPrNanNNDP58a1r5ehhM"
reviews_sheet_id = "1qS_5S0zephNVV2FcvwdFlRRDiMhpOGsGhfyzym-Qg_U"

# Open the controller sheet
controller_sheet = client.open_by_key(controller_sheet_id).sheet1

# Open the reviews spreadsheet
reviews_workbook = client.open_by_key(reviews_sheet_id)

def fetch_res_id(restaurant_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    response = requests.get(restaurant_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    script_tag = soup.find('script', string=re.compile("resId"))
    if script_tag:
        html_response = script_tag.string
        cleaned_content = html_response.replace('\\', '')
        match = re.search(r'"resId":(\d+)', cleaned_content)
        return match.group(1) if match else None
    return None

def fetch_reviews(res_id, page):
    url = f"https://www.zomato.com/webroutes/reviews/loadMore?sort=dd&filter=reviews-dd&res_id={res_id}&page={page}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    response = requests.get(url, headers=headers)
    out = response.json()
    return [
        {
            'reviewID': review.get('reviewId'),
            'userName': review.get('userName'),
            'timestamp': review.get('timestamp'),  # Keep this as a string
            'reviewText': review.get('reviewText'),
            'rating': review.get('ratingV2'),
            'dining/delivery type': review.get('experience')
        }
        for review in out['entities']['REVIEWS'].values()
    ], out['page_data']['sections']['SECTION_REVIEWS']['numberOfPages']

def convert_relative_time(relative_time):
    """Convert relative timestamps (e.g., '23 hours ago', 'yesterday', 'one month ago') to exact dates."""
    now = datetime.now()

    # Handle specific strings first
    if "yesterday" in relative_time.lower():
        return now - timedelta(days=1)

    if "one month" in relative_time.lower():
        return now - relativedelta(months=1)

    if "just now" in relative_time.lower():
        return now

    # General case for numeric times
    parts = relative_time.split()
    try:
        quantity = int(parts[0])
        if "hour" in relative_time:
            return now - timedelta(hours=quantity)
        elif "day" in relative_time:
            return now - timedelta(days=quantity)
        elif "month" in relative_time:
            return now - relativedelta(months=quantity)
        else:
            raise ValueError("Unrecognized relative time format.")

    except (ValueError, IndexError):
        return None  # Return None for unrecognized formats

def parse_absolute_date(date_str):
    """Parse absolute date strings like 'Oct 29, 2023' into datetime objects."""
    return datetime.strptime(date_str, "%b %d, %Y")

def dump(restaurant_url):
    res_id = fetch_res_id(restaurant_url)
    if not res_id:
        print("res_id not found.")
        return None, None, None

    # Create or open a sheet for this restaurant in the reviews workbook
    try:
        sheet = reviews_workbook.add_worksheet(title=res_id, rows="100", cols="20")
    except gspread.exceptions.APIError:
        sheet = reviews_workbook.worksheet(res_id)
        sheet.clear()

    # Updated headers to include 'original_timestamp' and 'datetime'
    headers = ['reviewID', 'userName', 'original_timestamp', 'datetime', 'reviewText', 'rating', 'dining/delivery type']
    sheet.insert_row(headers, 1)

    # Fetch and insert all reviews
    all_reviews = []
    page = 1
    while True:
        reviews, no_of_pages = fetch_reviews(res_id, page)
        all_reviews.extend(reviews)
        page += 1
        if page > no_of_pages:
            break
        time.sleep(1)

    rows = [[
        review['reviewID'],
        review['userName'],
        review['timestamp'],  # Original timestamp as a string
        (parse_absolute_date(review['timestamp']).isoformat()
         if re.match(r"^[A-Za-z]{3} \d{1,2}, \d{4}$", review['timestamp'])
         else convert_relative_time(review['timestamp']).isoformat() if convert_relative_time(review['timestamp']) else None),  # Converted datetime
        review['reviewText'],
        review['rating'],
        review['dining/delivery type']
    ] for review in all_reviews]

    sheet.append_rows(rows, value_input_option='RAW')
    latest_review_id = rows[0][0] if rows else None
    latest_review_date = rows[0][3] if rows else None  # Updated to get the converted datetime

    print(f"Dump complete for {restaurant_url}. Latest review ID: {latest_review_id}, Date: {latest_review_date}")
    return latest_review_id, latest_review_date, res_id, len(all_reviews)

def incremental(restaurant_url, latest_review_id, current_total_reviews):
    res_id = fetch_res_id(restaurant_url)
    if not res_id:
        print("res_id not found.")
        return None, None, 0

    try:
        sheet = reviews_workbook.worksheet(res_id)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Sheet for restaurant {res_id} not found.")
        return None, None, 0

    new_reviews = []
    page = 1
    new_latest_review_id = latest_review_id
    new_latest_review_date = None
    found_latest = False

    while not found_latest:
        reviews, _ = fetch_reviews(res_id, page)
        for review in reviews:
            review_id = review['reviewID']
            if review_id == latest_review_id:
                found_latest = True
                break
            new_reviews.append([
                review_id,
                review['userName'],
                review['timestamp'],  # Original timestamp as a string
                (parse_absolute_date(review['timestamp']).isoformat()
                 if re.match(r"^[A-Za-z]{3} \d{1,2}, \d{4}$", review['timestamp'])
                 else convert_relative_time(review['timestamp']).isoformat() if convert_relative_time(review['timestamp']) else None),  # Converted datetime
                review['reviewText'],
                review['rating'],
                review['dining/delivery type']
            ])
            if not new_latest_review_date:
                new_latest_review_id = review_id
                new_latest_review_date = review['timestamp']
        page += 1
        time.sleep(1)

    if new_reviews:
        sheet.append_rows(new_reviews, value_input_option='RAW')
        print(f"Appended {len(new_reviews)} new reviews.")
    else:
        print("No new reviews found.")

    return new_latest_review_id, new_latest_review_date, len(new_reviews)

if __name__ == "__main__":
    # Fetch data from the controller sheet
    rows = controller_sheet.get_all_records()

    for i, row in enumerate(rows, start=2):
        restaurant_url = row['Restaurant URL']
        dump_function = row['Dump Function (Start/Stop)']
        incremental_function = row['Incremental Function (Start/Stop)']

        if dump_function.lower() == "start":
            latest_review_id, latest_review_date, res_id, total_reviews = dump(restaurant_url)
            controller_sheet.update(range_name=f"D{i}", values=[[res_id]])  # Update ResID
            controller_sheet.update(range_name=f"E{i}", values=[[latest_review_id]])  # Update latest review ID
            controller_sheet.update(range_name=f"F{i}", values=[[latest_review_date]])  # Update latest review date
            controller_sheet.update(range_name=f"G{i}", values=[[total_reviews]])  # Update total reviews pulled

        elif incremental_function.lower() == "start":
            latest_review_id = row['Latest Review ID']
            current_total_reviews = row['Total Reviews Pulled']
            new_latest_review_id, new_latest_review_date, new_reviews_count = incremental(restaurant_url, latest_review_id, current_total_reviews)
            controller_sheet.update(range_name=f"D{i}", values=[[res_id]])  # Update ResID
            controller_sheet.update(range_name=f"E{i}", values=[[new_latest_review_id]])  # Update latest review ID
            controller_sheet.update(range_name=f"F{i}", values=[[new_latest_review_date]])  # Update latest review date
            controller_sheet.update(range_name=f"G{i}", values=[[current_total_reviews + new_reviews_count]])  # Update total reviews pulled
