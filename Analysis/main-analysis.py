import pandas as pd
import pymongo
from rich.console import Console

console = Console()

mongoClient = pymongo.MongoClient("localhost", 27017)

coll = mongoClient["test-database"]["test-collection"]

res_coll = mongoClient["test-database"]["test-analysis"]

List_HNX30 : list = ["CAP","CEO","DHT","DTD","DVM","DXP","HLD","HUT","IDC","IDV"
     ,"L14","L18","LAS","LHC","MBS","NTP","NVB","PLC","PSD","PVB"
    ,"PVC","PVS","SHS","SLS","TIG","TMB","TNG","TVD","VC3","VCS"]

def tinh_toan_thi_truong():
    tt = list()
    listsym = dict()
    listVol = dict()
    res_coll.drop()

    for sym in List_HNX30:
        listsym.update({sym: 0.0})
        listVol.update({sym: 0})
    df = pd.DataFrame(list(coll.find({'id': 3220}))).loc[ : , ['sym', 'lastVol', 'lastPrice', 'timeServer'] ]

    point : float = 0.0

    totalVol : int = 0

    for val in df.values:
        totalVol += val[1]
        
        point += float(val[2]) - listsym[val[0]]
        point = round(point,1)
        
        listsym[val[0]] = val[2]
        listVol[val[0]] += val[1]
      
        # tt.append({
        #     'timeServer': val[3],
        #     'point':point,
        #     'totalVol': totalVol,
        #     'listsym': {**listsym},
        #     'listVol': {**listVol}
        # })
        res_coll.insert_one({
            'timeServer': val[3],
            'point':point,
            'totalVol': totalVol,
            'listsym': {**listsym},
            'listVol': {**listVol}
        })
       
if __name__ == "__main__":
    
    tinh_toan_thi_truong()