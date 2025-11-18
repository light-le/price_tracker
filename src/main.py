from datetime import datetime
from os import environ
from random import random
import re
from string import ascii_uppercase
from time import sleep

import google.auth
from googleapiclient.discovery import build
import requests

SHEET_ID = environ['SHEET_ID']

def read_sheet(sheet_id: str, sheet_range: str) -> list[list]:
    creds, _ = google.auth.default()
    service = build("sheets", "v4", credentials=creds)

    result = (
        service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=sheet_range)
            .execute()
    )
    rows = result["values"]
    return rows

def sheet_list_of_lists_to_list_of_dicts(lol: list[list]) -> list[dict]:
    header, *rows = lol
    return [
        {
            header: data for header, data in zip(header, row)
        }
        for row in rows
    ]
    
def clean_up_prices(price: str) -> int:
    '''
    1.099.000 Đ -> 1099000
    '''
    return int(price.replace('Đ', '').strip().replace('.', ''))
    
def regex_extract_price(url: str, regex: str) -> int:
    # sleep(random())
    page = requests.get(url)
    
    match = re.search(regex, page.text)
    
    try:
        raw_price = match.group(1)
        return clean_up_prices(raw_price)
    except IndexError as e:
        print(f"could not find {regex} in {url}")
        raise e



def write_current_prices_to_sheet(sheet_id: str, current_prices: list[int], sheet_range: str):
    creds, _ = google.auth.default()
    service = build("sheets", "v4", credentials=creds)
    values = [current_prices]
    body = {"values": values, "majorDimension": "COLUMNS"}
    result = (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=sheet_id,
            range=sheet_range,
            valueInputOption="USER_ENTERED",
            body=body,
        )
        .execute()
    )
    return result

def append_values(spreadsheet_id, range_name, values):
    """
    Creates the batch_update the user has access to.
    Load pre-authorized user credentials from the environment.
    """
    creds, _ = google.auth.default()
        
    service = build("sheets", "v4", credentials=creds)
    body = {"values": values}
    result = (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            body=body,
        )
        .execute()
    )
    print(f"{(result.get('updates').get('updatedCells'))} cells appended.")
    return result


def main():
    rows = read_sheet(SHEET_ID, 'items')
    headers, *_ = rows
    items = sheet_list_of_lists_to_list_of_dicts(rows)
    print("Successfully query items from google sheet")
    
    item_ids = [item['id'] for item in items]
    
    current_price_column_letter = ascii_uppercase[headers.index('current_price')]
    
    current_prices = [regex_extract_price(item['url'], item['discount_price_regex']) for item in items]
    print(f"current prices have been successfully queried: {current_prices}")
    
    range = f"{current_price_column_letter}2:{current_price_column_letter}{str(1+len(current_prices))}"
    write_current_prices_to_sheet(SHEET_ID, current_prices, range)
    print(f"current prices have been successfully updated in range {range}")
    
    history_append = [
        [
            id, price, datetime.now().isoformat()
        ]
        for id, price in zip(item_ids, current_prices)
    ]
    
    append_values(spreadsheet_id=SHEET_ID, range_name="history", values=history_append)
    print(f"Successfully append {len(history_append)} rows to the history sheet.")
    
    

if __name__ == '__main__':
    main()