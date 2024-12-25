import pandas as pd
from rich.console import Console
import datetime, time
import json
import pymongo
from time import process_time


mongo_client = pymongo.MongoClient("localhost", 27017)


coll = mongo_client["project3-database"]["2024-07-26"]

insertdata = []

console = Console()


List_HNX30 : list = ["CAP","CEO","DHT","DTD","DVM","DXP","HLD","HUT","IDC","IDV"
     ,"L14","L18","LAS","LHC","MBS","NTP","NVB","PLC","PSD","PVB"
    ,"PVC","PVS","SHS","SLS","TIG","TMB","TNG","TVD","VC3","VCS"]



class SYM:
    def __init__(self , sym: str):
        self.sym = sym
        
        self.lastPrice : float = 0
        self.lastVol : int = 0
        self.totalVol : int = 0
        self.Time : datetime = datetime.datetime(2024,7,26,0,0,0)
        self.TimeServer : datetime = datetime.datetime(2024,7,26,0,0,0)
        self.listStock : list = list()
        
        self.listBoard1 : list = list()
    

    # check goi tin bi trung lap    
    def check_duplicate_Stock(self, lastVol: int , lastPrice: int ,totalVol: int , Time: datetime, TimeSer: datetime) -> bool:
        msgInfo: dict = {
            lastVol,
            lastPrice,
            totalVol,
            Time,
            TimeSer
        }
        
        if msgInfo in self.listStock:
            return False
        
        deltime : datetime = TimeSer - datetime.timedelta(seconds= 1)
        msgInfo2 : dict ={
            lastVol,
            lastPrice,
            totalVol,
            Time,
            deltime
        }

        if msgInfo2 in self.listStock :
            return False
        

        
        self.listStock.append(msgInfo)
        return True
    
    def check_duplicate_Board_1( self, side : str, g1: str, g2 : str, g3: str, timeServer: datetime ) -> bool:
        msgInfo : dict = {
            side,
            g1,
            g2,
            g3,
            timeServer
        }

        if msgInfo in self.listBoard1 :
            return False
        
        
        deltime : datetime = TimeSer + datetime.timedelta(seconds= 1)
        msgInfo2 : dict ={
            side,
            g1,
            g2,
            g3,
            deltime
        }

        if msgInfo2 in self.listBoard1 :
            return False
        
        self.listBoard1.append(msgInfo)
        return True
    
    def check_duplicate_Board_2( self, ) -> bool:
        ...

    # Kiem tra logic goi tin
    def check_Time ( self, Time: time) -> bool:
        if( self.Time >= Time):
            return False
        
        self.Time = Time
        
        return True
    
    def check_Time_Server ( self, TimeSer: time) -> bool:
        if( self.TimeServer >= TimeSer):
            return False
        
        self.TimeServer = TimeSer
        
        return True
    
    def check_lastVol( self, lastVol: int , totalVol: int ) -> bool:
        if( self.totalVol + lastVol != totalVol):
            return False

        self.lastVol = lastVol
        self.totalVol = totalVol 
        
        return True
    

if __name__ == "__main__":
    df_AISEC = pd.read_csv(r'D:\DAI HOC\PROJ3\SAMPLE DATA\AISEC-HNX_2024-07-26.csv', header= None)
    df_VPS = pd.read_csv(r'D:\DAI HOC\PROJ3\SAMPLE DATA\VPS-HNX_2024-07-26.csv', header= None)
    
    data = pd.concat([ df_AISEC , df_VPS] , axis= 'index').sort_values(0)
    
    #data = data.head(5000)
    #data = data.tail(60000)
    
    df_collected = pd.DataFrame({'0':[],'1':[],'2':[],'3':[]})
    
    ListSymJoin = list()
    
    ListSymObj = dict()
    
    for sym in List_HNX30:
        ListSymJoin.append( sym )
        obj : SYM = SYM( sym) 
        ListSymObj[ sym ]  = obj

    t1_start = process_time() 
    
    for x in data.values:
        try:
            value = json.loads(x[3][2:])            
            # if( value[0] == 'regs'):
                
            #     msg = json.loads(value[1])
            #     listmsg : list = msg['list'].split(',')
           
            #     for sym in listmsg:
            #         if (sym not in ListSymJoin) & (sym in List_HNX30): 
            #             ListSymJoin.append( sym )
            #             obj : SYM = SYM( sym) 
            #             ListSymObj[ sym ]  = obj
                
            ### id 3220 ###           
            if( value[0] == 'stock' ):
               
                msg = value[1][ 'data' ]
                
                sym = msg[ 'sym' ]
                id = msg[ 'id' ]
                
                if sym not in List_HNX30:
                    continue 
                
                # if( sym != 'CAP'): continue
                # console.log(msg)
                if( 'sID' not in msg ) | ('time' not in msg):
                    continue
                
                obj : SYM = ListSymObj[ sym ]

                #console.log(msg)
                if(  msg['sID'] is None ) | (msg['time'] is None):
                    continue
                
                Ti = msg['time'].split(':')
                Time = datetime.datetime(  2024,7,26,  int(Ti[0] ), int(Ti[1] ), int(Ti[2] ))
                TiS = msg['timeServer'].split(':')
                TimeSer = datetime.datetime(   2024,7,26,  int(TiS[0] ), int(TiS[1] ), int(TiS[2] ))
                
                # Goi tin loi
                
                
                # Check goi tin trung lap
                if( obj.check_duplicate_Stock( msg[ 'lastVol'], msg['lastPrice'] ,msg['totalVol'] , Time, TimeSer) == False):
                    #console.log('dup')
                    continue
                
                # Check logic goi tin
                elif( obj.check_Time ( Time ) == False):
                    #console.log(msg, 'Stock lech time')
                    continue
                
                elif( obj.check_lastVol( msg[ 'lastVol'], msg['totalVol']) == False ):
                    #console.log(msg, 'Stock lech Vol')
                    continue
                
                
                insertdata.append(msg)

                try:
                    df_collected.loc[len(df_collected.index)] = [x[0], x[1], x[2], x[3]]
                
                except:
                    pass
                    
                    
            ### id 3210 ###
            elif( value[0] == 'board'):
                msg = value[1][ 'data' ]
                
                sym = msg[ 'sym' ]
                id = msg[ 'id' ]
                if( id == 3220):
                    continue
                if sym not in List_HNX30:
                    continue 
                
                obj : SYM = ListSymObj[ sym ]
                
                TiS = msg['timeServer'].split(':')
                TimeSer = datetime.datetime(   2024,7,26,  int(TiS[0] ), int(TiS[1] ), int(TiS[2] ))
                
                if 'g1' in msg:
                    
                    if obj.check_duplicate_Board_1(msg['side'], msg['g1'], msg['g2'], msg['g3'], TimeSer) == False:
                        #console.log(msg, 'dup')
                        continue

                elif 'ndata' in msg:
                    
                    continue

                elif 'BVolume' in msg:
                        continue
                
                insertdata.append(msg)
                
                try:
                    df_collected.loc[len(df_collected.index)] = [x[0], x[1], x[2], x[3]]
                
                except:
                    pass
                
            else:
                continue
                
        except:
            pass

    coll.insert_many(insertdata)

    console.log("check done")
    
    df_collected.to_csv("HNX_COLLECTED_2024-07-26.csv",index= False,header= None)
    
    t1_stop = process_time()
    console.log( t1_stop - t1_start)