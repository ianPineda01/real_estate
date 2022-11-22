from pyspark.sql import SparkSession
from flask import Flask, render_template, redirect
from flask_wtf import FlaskForm #type: ignore Stub file not found 
from wtforms import StringField #type: ignore Stub file not found
from wtforms.validators import DataRequired, URL #type: ignore Stub file not found

import webbrowser

from scraping import scrape, html_from_url
from db import write_DF, read_to_DF, clear_data

DATABASE_FILE = 'main.db'

app = Flask(__name__)
app.config['SECRET_KEY'] = 'TO-DO: have a real key'

class FetchURL(FlaskForm):
    url = StringField('url:', validators=[DataRequired(), URL()])

@app.route('/')
def index():
    """
    Flask index route, creates a flask_wtform to get the desired url for querying.
    """
    form = FetchURL()
    return render_template('index.html', form=form) 


@app.route('/clearData', methods=['GET', 'POST'])
def clear_db():
    """
    """
    clear_data(DATABASE_FILE)
    return redirect('/')

@app.route('/dbData', methods=['GET', 'POST'])
def db_data():
    """
    Reads data from database and returns dataframe as html
    """
    spark = SparkSession.builder.appName('RealEstate').getOrCreate()
    df = read_to_DF(spark, DATABASE_FILE)
    if df is None:
        return 'No data avaliable'
    else:
        return df.toPandas().to_html()

@app.route('/fetchURL', methods=['GET', 'POST'])
def fetch_URL():
    """
    Flask fetchURL route, queries the website from the index form, scrapes it for
    relevant data, and makes a spark dataframe with said data, finally returns a
    table with the data gathered.
    """
    spark = SparkSession.builder.appName('Real_Estate').getOrCreate()

    form = FetchURL()

    url:str = form.url.data
    html:str = html_from_url(url)
    data = scrape(html)

    df = spark.createDataFrame(data, ['Size', 'Price (MXN)', 'Link'])
    df2 = df.withColumn('Price/m^2', df['Price (MXN)'] / df['Size'])

    write_DF(df2, DATABASE_FILE)
    resulting_html = df2.toPandas().to_html(index=False) 
    spark.stop()
    return resulting_html

def main():
    """
    Entry point to the app.
    Opens a web browser at index page of web server, and starts flask server
    """
    webbrowser.open('localhost:5000', new = 2)
    app.run(debug=True)

if __name__ == '__main__':
    main()