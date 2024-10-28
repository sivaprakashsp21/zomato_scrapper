import requests
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import re
import time

# Google Sheets setup
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)
sheet_id = "1qS_5S0zephNVV2FcvwdFlRRDiMhpOGsGhfyzym-Qg_U"
workbook = client.open_by_key(sheet_id)


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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    out = response.json()
    return [
        {
            'reviewID': review.get('reviewId'),
            'userName': review.get('userName'),
            'timestamp': review.get('timestamp'),
            'reviewText': review.get('reviewText'),
            'rating': review.get('ratingV2'),
            'dining/delivery type': review.get('experience')
        }
        for review in out['entities']['REVIEWS'].values()
    ], out['page_data']['sections']['SECTION_REVIEWS']['numberOfPages']


def dump(restaurant_url):
    res_id = fetch_res_id(restaurant_url)
    if not res_id:
        print("res_id not found.")
        return None, None

    # Create or open a sheet for this restaurant
    try:
        sheet = workbook.add_worksheet(title=res_id, rows="100", cols="20")
    except gspread.exceptions.APIError:
        sheet = workbook.worksheet(res_id)
        sheet.clear()

    headers = ['reviewID', 'userName', 'timestamp', 'reviewText', 'rating', 'dining/delivery type']
    sheet.insert_row(headers, 1)

   
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
        review['timestamp'],
        review['reviewText'],
        review['rating'],
        review['dining/delivery type']
    ] for review in all_reviews]

    sheet.append_rows(rows, value_input_option='RAW')
    latest_review_id = rows[0][0]
    latest_review_date = rows[0][2]

    print(f"Dump complete. Latest review ID: {latest_review_id}, Date: {latest_review_date}")
    return latest_review_id, latest_review_date


def incremental(restaurant_url, latest_review_id):
    res_id = fetch_res_id(restaurant_url)
    if not res_id:
        print("res_id not found.")
        return None, None

    try:
        sheet = workbook.worksheet(res_id)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Sheet for restaurant {res_id} not found.")
        return None, None

    new_reviews = []
    page = 1
    new_latest_review_id = latest_review_id
    new_latest_review_date = None
    found_latest = False

    while not found_latest:
        reviews, _ = fetch_reviews(res_id, page)
        for review in reviews:
            review_id = review['reviewID']
            review_date = review['timestamp']
            if review_id == latest_review_id:
                found_latest = True
                break
            new_reviews.append([
                review_id,
                review['userName'],
                review_date,
                review['reviewText'],
                review['rating'],
                review['dining/delivery type']
            ])
            if not new_latest_review_date:
                new_latest_review_id = review_id
                new_latest_review_date = review_date
        page += 1
        time.sleep(1)

    if new_reviews:
        sheet.append_rows(new_reviews, value_input_option='RAW')
        print(f"Appended {len(new_reviews)} new reviews.")
    else:
        print("No new reviews found.")

    return new_latest_review_id, new_latest_review_date



if __name__ == "__main__":
    while True:
        restaurant_url = input("Enter restaurant URL (or 'stop' to end): ")
        if restaurant_url.lower() == "stop":
            break

        # Full scrape if it's the first time
        latest_review_id, latest_review_date = dump(restaurant_url)

        while True:
            choice = input("Check for new reviews? (y/n): ")
            if choice.lower() != 'y':
                break
            latest_review_id, latest_review_date = incremental(restaurant_url, latest_review_id)
            print(f"Updated latest review ID: {latest_review_id}")
            print(f"Updated latest review date: {latest_review_date}")
