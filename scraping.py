from bs4 import BeautifulSoup, Tag
import requests

from typing import List, Tuple
import re

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
    if(len(html.history) > 0): 
        return ''
    else:
        return html.text

def scrape(html:str) -> List[Tuple[int, int, str]]:
    """
    Takes the html from an inmuebles24.com query as a string, scrapes the page for
    desired info, then returns a List of Tuples from it
    """
    soup = BeautifulSoup(html, 'lxml')

    postings = soup.find_all('div', class_ = 'sc-i1odl-0 cYmZqs')
    prices = [x.find('div', class_='sc-12dh9kl-4 ehivnq') for x in postings]
    prices = [price_to_int(x) for  x in prices]

    square_metres = [x.find('div', class_ = 'sc-1uhtbxc-0 cIDnkN') for x in postings]
    square_metres = [x.find_all('span')[1] for x in square_metres]
    square_metres = [metres_to_int(x) for x in square_metres]

    links = [x.get('data-to-posting') for x in postings]
    links = [f'www.inmuebles24.com{x}' for x in links] 

    return list(zip(square_metres, prices, links))
