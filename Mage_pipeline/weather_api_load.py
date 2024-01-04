import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@data_loader
def load_data_from_api(*args, **kwargs):
    all_data = []
    page_size = 100
    has_more_data = True
    page_num = 0

    while has_more_data:
        # Build the url and download the results
        url = f'https://data.bs.ch/api/explore/v2.1/catalog/datasets/100294/records?order_by=timestamp%20ASC&limit={page_size}&offset={page_num}'
        response = requests.get(url)
        data = response.json()

        # Check if there is more data
        has_more_data = len(data['results']) > 0

        # Append data to the result
        all_data += data['results']

        # Move to the next page
        page_num += page_size

    return pd.DataFrame(all_data)