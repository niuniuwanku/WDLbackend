import requests as requests
from bson import json_util
from flask import Flask, redirect, url_for, request, make_response, jsonify,abort
from flask_cors import CORS
import json
from pymongo import MongoClient
from bson.json_util import dumps
from datetime import datetime
import flask
import pymongo
app = Flask(__name__)
CORS(app)

# import requests
import urllib.parse

@app.route('/location',methods = ['POST', 'GET'])
def location():
    
   data = request.get_data()
   dict = json.loads(data.decode('utf-8'))
   # show(dict)
   print(dict)
   return dict


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["WDL"]
collection = mydb["WDLData"]

x = collection.find_one()
#
#
#
# client = MongoClient(port=27017)
# db = client.WDL
# collection = db.WDLData
print(x)



# print(collection.find()[0]["Date"]<collection.find()[1]["Date"])
result = collection.aggregate([
    {"$group": {
        "_id": {
            "Crime": "$Crime",
            "SubtypeCrime": "$SubtypeCrime",
            "Province":"$Province",
            "Canton":"$Canton",
            "District":"$District",
            "lat":"$lat",
            "long":"$long"
        },
        "Count": {"$sum": 1}
    }},

])
latlonglist =[]
for i in list(result):
    latlongdict = {}
    latlongdict["lat"] = i["_id"]["lat"]
    latlongdict["long"] = i["_id"]["long"]
    latlongdict["Count"] = i["Count"]
    latlonglist.append(latlongdict)

latlongjson = dumps(latlonglist)
# print(latlongjson)
# print(list(result)[0])
json_data = dumps(list(result))
# print(json_data)
# for i in result:
#     print(i)
# print(len(latlonglist))
@app.route('/heatmap')
def getheatmapdata():
    return latlongjson


@app.route('/heatmapbydate')
def getheatmapdatabydate():
    Crimetype = request.args.get('Crimetype')
    result = collection.aggregate([
        {"$match":{"Crime":Crimetype}},
        {"$group": {
            "_id": {
                "Crime": "$Crime",
                "SubtypeCrime": "$SubtypeCrime",
                "Province": "$Province",
                "Canton": "$Canton",
                "District": "$District",
                "lat": "$lat",
                "long": "$long"
            },

            "Count": {"$sum": 1}
        }},

    ])
    latlonglist = []
    for i in list(result):
        latlongdict = {}
        latlongdict["a"] = i["_id"]["Crime"]
        latlongdict["lat"] = i["_id"]["lat"]
        latlongdict["long"] = i["_id"]["long"]
        latlongdict["Count"] = i["Count"]
        latlonglist.append(latlongdict)
    latlongjson = dumps(latlonglist)

    # return len(llresult)
    return latlongjson

@app.route('/date')
def getbydate():
    from_date = request.args.get('from_date')
    from_date = datetime.strptime(from_date, '%y-%m-%d')
    to_date = request.args.get('to_date')
    to_date = datetime.strptime(to_date, '%y-%m-%d')
    Crimetype = request.args.get('Crimetype')

    result = collection.aggregate([
        {"$match":
             {   "Crime":Crimetype,
                 "Date": {"$gte": from_date, "$lt": to_date}
             }},
        {"$group": {
            "_id": {
                "Crime": "$Crime",
                "SubtypeCrime": "$SubtypeCrime",
                "Province": "$Province",
                "Canton": "$Canton",
                "District": "$District",
                "lat": "$lat",
                "long": "$long"
            },

            "Count": {"$sum": 1}
        }}

      ])


    latlonglist = []
    for i in list(result):
        latlongdict = {}
        latlongdict["Crime"] = i["_id"]["Crime"]
        latlongdict["lat"] = i["_id"]["lat"]
        latlongdict["long"] = i["_id"]["long"]
        latlongdict["Count"] = i["Count"]
        latlonglist.append(latlongdict)
    latlongjson = dumps(latlonglist)
    print(latlongjson)


    return latlongjson

@app.route('/dataall')
def getbydataall():
    from_date = request.args.get('from_date')
    from_date = datetime.strptime(from_date, '%y-%m-%d')

    to_date = request.args.get('to_date')
    to_date = datetime.strptime(to_date, '%y-%m-%d')

    result = collection.aggregate([
        {"$match":
             {
                 "Date": {"$gte": from_date, "$lt": to_date}
             }},
        {"$group": {
            "_id": {
                "Crime": "$Crime",
                "SubtypeCrime": "$SubtypeCrime",
                "Province": "$Province",
                "Canton": "$Canton",
                "District": "$District",
                "lat": "$lat",
                "long": "$long"
            },

            "Count": {"$sum": 1}
        }}

      ])

    latlonglist = []
    for i in list(result):
        latlongdict = {}
        latlongdict["Crime"] = i["_id"]["Crime"]
        latlongdict["lat"] = i["_id"]["lat"]
        latlongdict["long"] = i["_id"]["long"]
        latlongdict["Count"] = i["Count"]
        latlonglist.append(latlongdict)
    latlongjson = dumps(latlonglist)



    return latlongjson



if __name__ == '__main__':
   app.run(host="0.0.0.0", port="5000")
