#====================================Imports====================================
from pyspark.sql import SparkSession
from flask import Flask, render_template
from flask_wtf import FlaskForm #type: ignore Stub file not found 
from wtforms import StringField #type: ignore Stub file not found
from wtforms.validators import DataRequired, URL #type: ignore Stub file not found
from scraping import scrape
import webbrowser

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

    html:str = html_from_url(form.url.data) #type: ignore
    data = scrape(html)

    df = spark.createDataFrame(data, ['Size', 'Price (MXN)', 'Link'])
    df2 = df.withColumn('Price/m^2', df['Price (MXN)'] / df['Size'])

    resulting_html = df2.toPandas().to_html(index=False) #type: ignore 
    spark.stop()
    return resulting_html

if __name__ == '__main__':
    webbrowser.open('localhost:5000', new = 2)
    app.run(debug=True)