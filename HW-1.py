
import json
from pathlib import Path
import requests

if __name__ == "__main__":
    file_path = Path(__file__).parent.joinpath("categoria")
    if not file_path.exists():
        file_path.mkdir()


url = "https://5ka.ru/api/v2/categories/"
url_products = "https://5ka.ru/api/v2/special_offers/"
params = {'categories': None,
    "records_per_page": 20}

response: requests.Response = requests.get(url)
if response.status_code == 200:
    data = response.json()

for i in data:
    params['categories'] = i['parent_group_code']
    response: requests.Response = requests.get(url_products,  params=params)
    data_product = response.json()
    file_path = file_path.joinpath(f'{i["parent_group_name"]}.json')
    # print(params)
    file_path.write_text(json.dumps(data_product, ensure_ascii=False))
    file_path = Path(__file__).parent.joinpath("categoria")

