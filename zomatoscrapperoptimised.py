import requests
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import re
import time

def dump(restaurant_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Cookie': 'PHPSESSID=74d74a0c236a0bfb925f965324e29f9b'
    }

    # Send GET request to the restaurant URL
    response = requests.get(restaurant_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract res_id from the page's JavaScript content
    script_tag = soup.find('script', string=re.compile("resId"))
    if script_tag:
        html_response = script_tag.string
        cleaned_content = html_response.replace('\\', '')
        match = re.search(r'"resId":(\d+)', cleaned_content)
        res_id = match.group(1) if match else None

    if not res_id:
        print("res_id not found.")
        return

    # Review data fetching setup
    page_url = f"https://www.zomato.com/webroutes/reviews/loadMore?sort=dd&filter=reviews-dd&res_id={res_id}&page=1"
    response = requests.get(page_url, headers=headers)
    out = response.json()
    no_of_pages = out['page_data']['sections']['SECTION_REVIEWS']['numberOfPages']
    final_reviews = []

    # Fetch reviews from each page
    def fetch_reviews(page):
        url = f"https://www.zomato.com/webroutes/reviews/loadMore?sort=dd&filter=reviews-dd&res_id={res_id}&page={page}"
        response = requests.get(url, headers=headers)
        out = response.json()
        reviews = out['entities']['REVIEWS']
        return [
            {
                'reviewID': review.get('reviewId'),
                'userName': review.get('userName'),
                'timestamp': review.get('timestamp'),
                'reviewText': review.get('reviewText'),
                'rating': review.get('ratingV2'),
                'dining/delivery type': review.get('experience')
            }
            for review in reviews.values()
        ]

    for i in range(1, no_of_pages + 1):
        final_reviews.extend(fetch_reviews(i))
        time.sleep(1)  # Pause to avoid rate limits

    # Google Sheets setup
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    client = gspread.authorize(creds)

    # Open the Google Sheets workbook by ID
    sheet_id = "1qS_5S0zephNVV2FcvwdFlRRDiMhpOGsGhfyzym-Qg_U"
    workbook = client.open_by_key(sheet_id)

    # Create or update a sheet named after the restaurant's res_id
    try:
        sheet = workbook.add_worksheet(title=res_id, rows="100", cols="20")
    except gspread.exceptions.APIError:
        sheet = workbook.worksheet(res_id)
        sheet.clear()

    # Add headers and data
    headers = ['reviewID', 'userName', 'timestamp', 'reviewText', 'rating', 'dining/delivery type']
    sheet.insert_row(headers, 1)

    rows = [
        [
            review.get('reviewID', ''),
            review.get('userName', ''),
            review.get('timestamp', ''),
            review.get('reviewText', ''),
            review.get('rating', ''),
            review.get('dining/delivery type', '')
        ]
        for review in final_reviews
    ]

    # Append rows in chunks to avoid exceeding quota
    def chunk_data(data, chunk_size=500):
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    for chunk in chunk_data(rows):
        sheet.append_rows(chunk, value_input_option='RAW')
        time.sleep(60)

    # Display latest review details
    print(f"LatestReviewID: {rows[0][0]}")
    print(f"latestReviewDate: {rows[0][2]}")

if __name__ == "__main__":
    while True:
        restaurant_url = input("Enter restaurant URL (or 'stop' to end): ")
        if restaurant_url.lower() == "stop":
            break
        dump(restaurant_url)

