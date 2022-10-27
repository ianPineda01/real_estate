#====================================Imports====================================
from typing import List, Tuple
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup, Tag
import requests
import sys
import re
import time

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
    # TO-DO convert USD prices to MXN
    return int(
        re.sub(
            "[^0-9]", 
            "",
            str(input.encode_contents())
        )
    )

def html_from_url(url:str) -> str:
    """
    Takes the url from an inmuebles24.com query, and returns the content of the
    page as a string
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    html = requests.get(url, headers = headers)
    #This way we prevent a result from getting through when we hit a redirection
    return html.text

def get_metres_prices(html:str) -> List[Tuple[int, int]]:
    """
    Takes the html from an inmuebles24.com query as a string, scrapes the page for
    square metre amounts and prices, then returns a List of Tuples from it
    """
    soup = BeautifulSoup(html, 'lxml')

    prices = soup.find_all('div', class_ = 'sc-12dh9kl-4 ehivnq')
    prices = [price_to_int(x) for  x in prices]

    square_metres = soup.find_all('div', class_ = 'sc-1uhtbxc-0 cIDnkN')
    square_metres = [x.find_all('span')[1] for x in square_metres]
    square_metres = square_metres[-len(prices):]
    square_metres = [metres_to_int(x) for x in square_metres]

    return list(zip(square_metres, prices))

#=====================================Main======================================
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

if len(sys.argv) < 2:
    print("The correct way to call the program is:")
    print("python main.py [url]")
    exit()

if len(sys.argv) > 2:
    n = int(sys.argv[2])
else:
    n = 1

metres_prices:List[Tuple[int, int]] = []

for i in range(1, n + 1):
    time.sleep(0.5)
    url = sys.argv[1].replace('.html', f'-pagina-${i}.html')
    html = html_from_url(url)
    metres_prices += get_metres_prices(html)

df = spark.createDataFrame(metres_prices, ['m^2', 'Precio'])

df.withColumn('Precio/m^2', df['Precio']/df['m^2']).show()

spark.stop()