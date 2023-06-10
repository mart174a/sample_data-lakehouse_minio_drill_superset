import csv
from os.path import exists
import requests

def getAndstoreNewestAsCSV(fileName: str = "EnergiData"):
    """Gets data from energidataservice.dk and stores the newest entries in a csv file"""

    print("Started fetching and storing the newest data entry")

    response = requests.get(
    url='https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime?limit=2&offset=0&sort=Minutes5UTC%20DESC&timezone=dk')

    result = response.json()
    records = result.get('records', [])
    storeCSV(f"{fileName}.csv",records)

    print("Successfully stored the newest entry")


def storeCSV(filename,dictList):
    e = exists(filename)
    with open(filename, 'a', newline='') as f:  
        w = csv.DictWriter(f, dictList[0].keys())
        if not e:
            w.writeheader()
        w.writerows(dictList)

def getAndStoreFromDateAsCSV(date:int, month:int, year:int ,fileName: str = "EnergiDataFromDate"):
    """
    Gets all data from a specific date and forward from energidataservice.dk and stores it in a csv file
    """
   
    off = 0
    lim = 10000
   
    print(f"Started fetching and storing data from {date}/{month}-{year} to today..")

    while True:
        response = requests.get(
        url='https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime?sort=Minutes5UTC%20ASC&timezone=dk',
        params = {
            'limit':lim,
            'offset':off,
            'start':f"{year}-{month:02d}-{date:02d}T00:00"
            }
        )
        result = response.json()
        records = result.get('records', [])
        storeCSV(f"{fileName}.csv",records)
       
        stored = len(records)

        print(f"Stored : {stored} files. {off + stored} total")

        if stored<lim:
            print(f"\nSuccessfully stored {off+stored} files.")
            break

        off += lim