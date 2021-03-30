from pathlib import Path
import time
import requests
from urllib.parse import urljoin
import bs4
import pymongo


class MagnitParse:
    def __init__(self, start_url, mongo_url):
        self.start_url = start_url
        client = pymongo.MongoClient(mongo_url) #обращение к клиенту mongodb = подключились к серверу
        self.db = client["gb_parse_19_03_21"] # обращение к бд (в скобках имя дб) = указали к какой бд будем обращаться

    def get_response(self, url, *args, **kwargs):
        for _ in range(15): #кол-во попыток достучаться
            response = requests.get(url, *args, **kwargs)
            if response.status_code == 200:
                return response
            time.sleep(1)
        raise ValueError("URL DIE")

    def get_soup(self, url, *args, **kwargs) -> bs4.BeautifulSoup: #метод возвращает суп
        soup = bs4.BeautifulSoup(self.get_response(url, *args, **kwargs).text, "lxml")
        return soup

    @property
    def template(self):
        data_template = {
            "url": lambda a: urljoin(self.start_url, a.attrs.get("href", "/")),
            "product_name": lambda a: a.find("div", attrs={"class": "card-sale__title"}).text,
            "image_url": lambda a: urljoin(
                self.start_url, a.find("picture").find("img").attrs.get("data-src", "/")
            ),
            "discount_name": lambda a: a.find("div", attrs={"class": "card-sale__header"}).text,
            "discount_label": lambda a: a.find("div", attrs={"class": "label label_sm label_magnit card-sale__discount"}).text,
            "old_price": lambda a: (int(a.find('div', attrs={"class": "label__price label__price_old"}).
                                        find("span", attrs={"class": "label__price-integer"}).text)
                                    + int(a.find('div', attrs={"class": "label__price label__price_old"}).
                                          find("span", attrs={"class": "label__price-decimal"}).text)/100),

            "new_price": lambda a: (int(a.find('div', attrs={"class": "label__price label__price_new"}).
                                        find("span", attrs={"class": "label__price-integer"}).text)
                                    + int(a.find('div', attrs={"class": "label__price label__price_new"}).
                                          find("span", attrs={"class": "label__price-decimal"}).text)/100),

            "date_from": lambda a: self.data_metod(a),
            "date_to": lambda a: self.data_metod_2(a)
        }
        return data_template

    def data_metod(self,dates):
        dat = dates.find('div', attrs={"class": "card-sale__date"}).find_all("p")[0]
        date_str = (str(dat))[5:(len((str(dat))) - 4)]
        return date_str

    def data_metod_2(self,dates):
        dat = dates.find('div', attrs={"class": "card-sale__date"}).find_all("p")[1]
        date_str = (str(dat))[5:(len((str(dat))) - 4)]
        return date_str


    def run(self): #метод запуска
        for product in self._parse(self.get_soup(self.start_url)):
            self.save(product)

    def _parse(self, soup): #метод генератор
        products_a = soup.find_all("a", attrs={"class": "card-sale"}) #список всех продуктов
        for prod_tag in products_a:
            product_data = {} #пустой словарь
            for key, func in self.template.items():
                try: #не у всех продуктов есть имя и поэтому делаем except
                    product_data[key] = func(prod_tag)
                except (AttributeError, ValueError):
                    pass

                #     product_data[key] = func(prod_tag)
                # except Exception:
                #     pass
            yield product_data

    def save(self, data):
        collection = self.db["magnit-3"] #обращение к коллекции / условно "таблица" в бд
        collection.insert_one(data) #записываем в коллекцию данные


if __name__ == "__main__":
    url = "https://magnit.ru/promo/"
    mongo_url = "mongodb://localhost:27017"
    parser = MagnitParse(url, mongo_url)
    parser.run()
