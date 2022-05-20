import urllib

import pandas as pd
import requests
from pymongo import MongoClient


def uniq(list_dicts):
    return [dict(p) for p in set(tuple(i.items())
        for i in list_dicts)]


lat = []
long = []
data = pd.read_csv("datasets/costa_rica_crime_data_english/costa_rica_crimes_english.csv")
position = data[["Province","Canton","District"]].to_dict("records")
uniqueposition = uniq(position)
# print(data.groupby(["Province","Canton","District"]).count()["Unnamed: 0"])
# for i in range(len(uniqueposition)):
#     try:
#         address = "Costa Rica,"+uniqueposition[i]["Province"]+","+uniqueposition[i]["Canton"]+","+uniqueposition[i]["District"]
#         url = "https://maps.googleapis.com/maps/api/geocode/json?address="+ address + "&key=AIzaSyCIYL23c6kXpDdF5o8m5Y-OoAH5hH2nQUY"
#         print(i)
#         print(address)
#         response = requests.get(url).json()
#         lat.append(response['results'][0]['geometry']['location']["lat"])
#         long.append(response['results'][0]['geometry']['location']["lng"])
#         uniqueposition[i]["lat"]=response['results'][0]['geometry']['location']["lat"]
#         uniqueposition[i]["long"]=response['results'][0]['geometry']['location']["lng"]
#     except IndexError:
#         uniqueposition[i]["lat"] = "null"
#         uniqueposition[i]["long"] = "null"
# uniquepositiondata = pd.DataFrame.from_dict(uniqueposition)
# print(uniquepositiondata)
# uniquepositiondata.to_csv("locationdata.csv")
#
# def uniq(list_dicts):
#     return [dict(p) for p in set(tuple(i.items())
#         for i in list_dicts)]

# uniquepositiondata = pd.read_csv("locationdata.csv")
# datawithlatlong = pd.merge(data, uniquepositiondata,  how='outer', left_on=["Province","Canton","District"], right_on = ["Province","Canton","District"])
# datawithlatlong['long'] = datawithlatlong['long'].astype(float)
# datawithlatlong.to_csv("datawithlatlong.csv")




datawithlatlong = pd.read_csv("datawithlatlong.csv")
datawithlatlong = datawithlatlong[['Crime', 'SubtypeCrime', 'Date', 'Time',
       'SubtypeVictim', 'Age', 'Gender', 'Nationality', 'Province', 'Canton',
       'District', 'lat', 'long']]
datawithlatlong['Date']= pd.to_datetime(datawithlatlong['Date'],format='%d.%m.%y')
datawithlatlong.to_csv("datawithlatlong.csv")
# print(datawithlatlong)
# client = MongoClient(port=27017)
# db = client.backend
# mycol = db["WDLData"]
# db.WDLData.insert_many(datawithlatlong.to_dict('records'))