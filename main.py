#====================================Imports====================================
from typing import List, Tuple
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup, Tag
from flask import Flask, render_template
from flask_wtf import FlaskForm #type: ignore Stub file not found 
from wtforms import StringField #type: ignore Stub file not found
from wtforms.validators import DataRequired, URL #type: ignore Stub file not found
import pandas # type: ignore I don't directly access pandas, but it is required
# for .toPandas() method 
import requests
import re

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
    if(len(html.history) > 0): 
        return ''
    else:
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

#====================================Flask======================================
app = Flask(__name__)
app.config['SECRET_KEY'] = 'TO-DO: have a real key'

class MyForm(FlaskForm):
    url = StringField('url:', validators=[DataRequired(), URL()])


@app.route('/')
def index():
    form = MyForm()
    return render_template('index.html', form=form) 

@app.route('/submit', methods=['GET', 'POST'])
def submit():
    spark = SparkSession.builder.appName('Real_Estate').getOrCreate()
    form = MyForm()
    html = html_from_url(form.url.data) #type: ignore The type checker doesn't know 
    # but I can be sure this will be a string
    metres_prices = get_metres_prices(html)
    df = spark.createDataFrame(metres_prices, ['m^2', 'Price (MXN)'])
    df2 = df.withColumn('Price/m^2', df['Price (MXN)'] / df['m^2'])
    resulting_html = df2.toPandas().to_html(index=False) #type: ignore again, I'm 
    # sure this returns a string
    spark.stop()
    return resulting_html

#=====================================Main======================================
if __name__ == '__main__':
    app.run(debug=True)