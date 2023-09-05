# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt
from pathlib import Path

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
hawaii = Path(r'C:\Users\kylem\Desktop\Bootcamp_Assignments\Challenges\Challenge_10\sqlalchemy-challenge\Resources\hawaii.sqlite')
engine = create_engine(f"sqlite:///{hawaii}")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with = engine)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    """Here are the available API routes"""
    return(
        f"Available Routes:<br/>"
        f"Analysis of precipitation:  /api/v1.0/precipitation<br/"

        f"List of Stations:  /api/v1.0/stations<br/>"
        
        f"Temperature Observations Station USC00519281:  /api/v1.0/tobs<br/>"

        f"Start Date Averages:  /api/v1.0/<start><br/>"

        f"Aggregate Averages:  /api/v1.0/<start>/<end>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    # Design a query to retrieve the last 12 months of precipitation data 
    recent_date = session.query(measurement.date).order_by(measurement.date.desc()).first()

    # Starting from the most recent data point in the database.
    lastdate = dt.datetime.strptime(recent_date[0], '%Y-%m-%d')

    # Calculate the date one year from the last date in data set.
    querydate = dt.date(lastdate.year -1, lastdate.month, lastdate.day)

    # Perform a query to retrieve the data and precipitation scores
    sel = [measurement.date, measurement.prcp]
    result = session.query(*sel).\
    filter(measurement.date >= querydate).all()
    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    precipitation = pd.DataFrame(result, columns=['Date','Precipitation'])
    precipitation = precipitation.sort_values(['Date'], ascending=True)

    # Sort the dataframe by date
    precipitation = precipitation.to_dict('index')
    session.close()

    return jsonify(precipitation)


@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)

    #Return a lsit of Stations
    sel1 = [station.station, station.name, station.latitude, station.longitude, station.elevation]
    active = session.query(*sel1).all()
    session.close()
    
    #Create a Dataframe and turn the data frame into a dictionary
    stations = pd.DataFrame(active,columns=['station', 'name', 'latitude', 'longitude', 'elevation'])
    stations = stations.to_dict('index')
    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)

    # Design a query to retrieve the last 12 months of precipitation data 
    recent_date = session.query(measurement.date).order_by(measurement.date.desc()).first()
    lastdate = dt.datetime.strptime(recent_date[0], '%Y-%m-%d')
    querydate = dt.date(lastdate.year -1, lastdate.month, lastdate.day)

    #Query to return the temperatures from the  last year
    sel2 = [measurement.date, measurement.tobs]
    temp = session.query(*sel2).\
        filter(measurement.date >=querydate, measurement.station == 'USC00519281' ).\
        group_by(measurement.date).\
        order_by(measurement.date).all()
    session.close()

    tobs = pd.DataFrame(temp,columns=['date', 'tobs'])
    tobs = tobs.to_dict('index')
    return jsonify(tobs)

@app.route("/api/v1.0/<start>")
def start(start):
    session = Session(engine)

    temp_avg = session.query(func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)).\
        filter(measurement.date ==start).all()
    session.close()

    temp_avg = pd.DataFrame(temp_avg, columns=['Min', 'Max', 'Average'])
    temp_avg = temp_avg.to_dict('index')
    return jsonify(temp_avg)

@app.route("/api/v1.0/<start>/<end>")
def end(start, end):
    session = Session(engine)

    temp_avg = session.query(func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)).\
        filter(measurement.date >=start, measurement.date <= end).all()
    session.close()

    temp_avg = pd.DataFrame(temp_avg, columns=['Min', 'Max', 'Average'])
    temp_avg = temp_avg.to_dict('index')
    return jsonify(temp_avg)


if __name__ == "__main__":
    app.run(debug=True)