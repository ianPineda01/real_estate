#====================================Imports====================================
from typing import List, Tuple
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup, Tag
import requests

#===================================Functions===================================
def metres_to_int(input:Tag) -> int:
    """
    Takes a metres <span> Tag and converts it into an int
    """
    return int(
        str(input.encode_contents())
        .replace('b\' <!-- -->', '')
        .replace(' m\\xc2\\xb2<!-- --> \'', '')
    )

def price_to_int(input:Tag) -> int:
    """
    Takes a price <div> and converts it into an int
    """
    return int(
        str(input.encode_contents())
        .replace('b\'MN ', '')
        .replace('\'', '')
        .replace(',', '')
    )

def get_metres_prices(url:str) -> List[Tuple[int, int]]:
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    html = requests.get(url, headers = headers)
    #This way we prevent a result from getting through when we hit a redirection
    if(len(html.history) > 0): 
        return []

    soup = BeautifulSoup(html.text, 'lxml')

    prices = soup.find_all('div', class_ = 'sc-12dh9kl-4 ehivnq')
    prices = [price_to_int(x) for  x in prices]

    square_metres = soup.find_all('div', class_ = 'sc-1uhtbxc-0 cIDnkN')
    square_metres = [x.find_all('span')[1] for x in square_metres]
    square_metres = square_metres[-len(prices):]
    square_metres = [metres_to_int(x) for x in square_metres]

    return list(zip(square_metres, prices))

#=====================================Main======================================

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

metres_prices = get_metres_prices('https://www.inmuebles24.com/departamentos-en-venta-en-alvaro-obregon-pagina-300.html')

df = spark.createDataFrame(metres_prices, ['m^2', 'Precio'])

df.show()

spark.stop()