from pyspark.sql import SparkSession
from flask import Flask, render_template
from flask_wtf import FlaskForm #type: ignore Stub file not found 
from wtforms import StringField #type: ignore Stub file not found
from wtforms.validators import DataRequired, URL #type: ignore Stub file not found

import webbrowser

from scraping import scrape

app = Flask(__name__)
app.config['SECRET_KEY'] = 'TO-DO: have a real key'

class MyForm(FlaskForm):
    url = StringField('url:', validators=[DataRequired(), URL()])

@app.route('/')
def index():
    """
    Flask index route, creates a flask_wtform to get the desired url for querying.
    """
    form = MyForm()
    return render_template('index.html', form=form) 

@app.route('/submit', methods=['GET', 'POST'])
def submit():
    """
    Flask submit route, queries the website from the index form, scrapes it for
    relevant data, and makes a spark dataframe with said data, finally returns a
    table with the data gathered.
    """
    spark = SparkSession.builder.appName('Real_Estate').getOrCreate()

    form = MyForm()

    html:str = html_from_url(form.url.data) #type: ignore
    data = scrape(html)

    df = spark.createDataFrame(data, ['Size', 'Price (MXN)', 'Link'])
    df2 = df.withColumn('Price/m^2', df['Price (MXN)'] / df['Size'])

    resulting_html = df2.toPandas().to_html(index=False) #type: ignore 
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