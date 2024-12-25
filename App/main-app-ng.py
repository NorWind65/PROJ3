#!/usr/bin/env python3
from nicegui import ui
import pandas as pd
import json
import pymongo
from rich.console import Console
from datetime import datetime, timedelta
console = Console()

mongoClient = pymongo.MongoClient("localhost", 27017)

coll = mongoClient["test-database"]["test-collection"]

analysis_coll = mongoClient["test-database"]["test-analysis"]
 

List_HNX30 : list = ["CAP","CEO","DHT","DTD","DVM","DXP","HLD","HUT","IDC","IDV"
     ,"L14","L18","LAS","LHC","MBS","NTP","NVB","PLC","PSD","PVB"
    ,"PVC","PVS","SHS","SLS","TIG","TMB","TNG","TVD","VC3","VCS"]

freefloat : list = [ 0.8,0.8,0.6,0.7,0.80,0.55,0.45,0.40,0.70,0.60,
                    0.76,0.45,0.35,0.75,0.25,0.25,1.00,0.25,0.25,0.50,
                    0.65,0.50,0.95,0.50,0.65,0.35,0.70,0.35,0.45, 0.20 ]

dff = dict()

for i in range(0,30):
    dff.update({List_HNX30[i]:freefloat[i]})

class echart_line_and_bar:

    def __init__(
        self,
        ):
        ...

    def make_chart(
            self,
            chart_title = 'Line and Bar chart',
            x_name = 'Time',
            left_y_name = 'Volume',
            right_y_name = 'Index',
            chart_style = 'height: 600px; width: 90%'
        ):
        
        self.options = {
            'title' : {
                'text' : chart_title
            },
            'tooltip': {
                'trigger': 'axis',
                'axisPointer': {
                    'type': 'cross'
                }
            },
            'legend': {

            },
            'dataZoom': [
                {
                    'show': True,
                    'realtime': True,
                    'type': 'slider'
                },
                {
                    'filterMode': 'none',
                    'yAxisIndex':[0],
                    'type': 'slider',
                    'width':30,
                    'showDataShadow': 'false',
                    'left': 30
                }
            ],
          
            'xAxis': {
                'name': x_name,
                'type': 'category',
            },
            'yAxis': [
                {
                    'name': left_y_name,
                    'type': 'value',
                    'splitLine': {
                        'show': False
                    },
                    'min' : 0,
                    'max' : 1200
                },
                {
                    'name': right_y_name,
                    'type': 'value',
                    'scale': True
                }
            ],
            'series': []
        }

        self.chart = ui.echart(
            options = self.options
        )

        self.chart.style(chart_style)

        return self.chart

    def add_trace(
        self,
        trace: dict,
        *args, **kwargs
        ):

        self.chart.options['series'].append(trace)
        self.chart.update()
        
    def update_trace(
        self,
        trace_name: str,
        data: list
        ):

        try:
            for trace in self.chart.options['series']:
                if trace['name'] == trace_name:
                 
                    trace['data'] = data
                    self.chart.run_chart_method(
                        ':setOption',
                        json.dumps(
                            {
                                'series': self.chart.options['series']
                            }
                        )
                    )
        except Exception as e:
            print(e)     
    def update_scale(
        self,
        yname: str,
        data: list
        ):
        try:
            for trace in self.chart.options['yAxis']:
                if trace['name'] == yname:
                    trace['min'] = data[0]
                    trace['max'] = data[1] 
                    self.chart.run_chart_method(
                        ':setOption',
                        json.dumps(
                            {
                                'yAxis': self.chart.options['yAxis']
                            }
                        )
                    )
        except Exception as e:
            print(e)
        #self.chart.update()
    def update_table(
        self,
        table: str
        ):
        dataSYMTK =  pd.DataFrame(list(coll.find({'sym': table , 'id': 3220})))
        
        self.update_trace(trace_name='Price' ,        data= tranferdata(dataSYMTK.loc[: ,  ['timeServer','lastPrice']]))
        self.update_trace(trace_name='Stock Volume' , data= tranferdata(dataSYMTK.loc[: ,  ['timeServer','lastVol'  ]]))
        
        Min  = dataSYMTK.loc[: ,  ['lastPrice']].values.min() - 1
        Max  = dataSYMTK.loc[: ,  ['lastPrice']].values.max() + 1 
        Min = round(Min,1)
        Max = round(Max,1)
        self.update_scale(yname='Price' , data=[Min , Max])
        
        self.chart.update()
        
        try:
            self.chart.run_chart_method(
                        ':setOption',
                        json.dumps(
                            {
                                'title' : {
                                    'text' : f'Lượng giao dịch Mã Chứng Khoán {table} trong ngày'
                                }
                            }
                        )
            )
         
        except Exception as e:
            print(e)
            
class echart_bang_gia:
    
    def __init__(self):
        ...

    def make_chart(self,
            chart_title = 'Line and Bar chart',
            x_name = 'Time',
            left_y_name = 'Volume',
            chart_style = 'height: 600px; width: 100%'
            ):
        self.options = {
            'title' : {
                'text' : chart_title
            },
            'tooltip': {
                'trigger': 'axis',
                'axisPointer': {
                    'type': 'cross'
                }
            },
            'legend': {

            },
            'dataZoom': [
                {
                    'show': True,
                    'realtime': True,
                    'type': 'slider'
                }
            ],
          
            'xAxis': {
                'name': x_name,
                'type': 'category',
            },
            'yAxis': [
                {
                    'name': left_y_name,
                    'type': 'value',
                    'splitLine': {
                        'show': False
                    },
                }
            ],
            'series': []
        }
         
        self.chart = ui.echart(
            options = self.options
        )

        self.chart.style(chart_style)

        return self.chart

    def add_trace(
        self,
        trace: dict,
        *args, **kwargs
        ):
        self.chart.options['series'].append(trace)
        self.chart.update()
     
    def update_trace(
        self,
        trace_name: str,
        data: list
        ):

        try:
            for trace in self.chart.options['series']:
                if trace['name'] == trace_name:
                 
                    trace['data'] = data
                    self.chart.run_chart_method(
                        ':setOption',
                        json.dumps(
                            {
                                'series': self.chart.options['series']
                            }
                        )
                    )
        except Exception as e:
            print(e)

    def update_table(
        self,
        table: str
        ):
        dataSYMTK =  pd.DataFrame(list(coll.find({'sym': table , 'id': 3210})))
        
        self.update_trace(trace_name='Buy Volume' ,  data= tranferdatabanggia_B(dataSYMTK))
        self.update_trace(trace_name='Sell Volume' , data= tranferdatabanggia_S(dataSYMTK))
        
        
        self.chart.update()
        
        try:
            self.chart.run_chart_method(
                        ':setOption',
                        json.dumps(
                            {
                                'title' : {
                                    'text' : f'Bảng Giá Mã Chứng Khoán {table} trong ngày'
                                }
                            }
                        )
            )
         
        except Exception as e:
            print(e)

class echart_pie():
    
    def __init__(
        self,
        ):
        ...
     
    def make_chart(self,
        chart_style = 'height: 600px; width: 100%'):
        self.options = {
            'tooltip': {
                'trigger': 'item'
            },
            'legend': {
                'orient': 'vertical',
                'left': 'left'
            }, 
            'series': []
        }

        self.chart = ui.echart(
            options = self.options
        )

        self.chart.style(chart_style)

        return self.chart

    def add_trace(
        self,
        trace: dict,
        *args, **kwargs
        ):
        self.chart.options['series'].append(trace)
        self.chart.update()

    def update_trace(self):
        ...

def tranferdatabanggia_B(df: pd.DataFrame):
    Buy = dict()

    for val in df.values:   
        for i in [4,5,6]:
            g = val[i].split('|')
            if(val[3] == 'B'):
                if( g[0] != '0.0') & (g[1] != '0' ) & ( g[0] != 'ATC'): Buy.update({float(g[0]) : int(g[1])}) 
            

    return sorted( Buy.items() )   
def tranferdatabanggia_S(df: pd.DataFrame):
    Sell = dict()
    
    for val in df.values:   
        for i in [4,5,6]:
            g = val[i].split('|')
            if(val[3] == 'S'):
                if( g[0] != '0.0') & (g[1] != '0') &  ( g[0] != 'ATC'): Sell.update({float(g[0]) : int(g[1])*-1}) 
    return sorted( Sell.items() )    
def tranferdata ( df : pd.DataFrame):
    return [
        (
            val[0],
            val[1]
        ) for val in df.values
        
    ] 

def infoSYM( sym: str):
    dfsym = pd.DataFrame(list(coll.find({'sym':  sym , 'id': 3220})))
    
    return [
        {'info':'Tổng số giao dịch', 'value':int(dfsym['totalVol'].count())},
        {'info':'Tổng khối lượng giao dịch' , 'value': int(dfsym['totalVol'].iloc[-1])},
        {'info':'Mức giá đầu ngày' , 'value': float(dfsym['lastPrice'].iloc[0])},
        {'info':'Mức giá cuối ngày' , 'value': float(dfsym['lastPrice'].iloc[-1])},
        {'info':'Phần trăm chênh lệch giá' , 'value': f'{round(((float( dfsym['lastPrice'].iloc[-1])/float(dfsym['lastPrice'].iloc[0]) - 1 ) * 100 ),2)} %'}
    ]

def infoSYM2( sym: str):
    dfsym = pd.DataFrame(list(coll.find({'sym':  sym , 'id': 3210})))
    
    Blist = tranferdatabanggia_B(dfsym)
    Bvol: int = 0
    for value in Blist:
        Bvol += int(value[1])
        
    Slist = tranferdatabanggia_S(dfsym)
    Svol: int = 0
    for value in Slist:
        Svol += int(value[1])
    
    return [
        {'info':'Tổng khối lượng mua', 'value': Bvol},
        {'info':'Tổng khối lượng bán', 'value': Svol},
    ]
       
def tytrongGia(df : pd.DataFrame):
    listgia: dict = dict(dict(df['listsym'].iloc[-1]))       
    listgia = sorted(listgia.items(), key=lambda x:x[1], reverse=True)
    return[
        (
            {
                'value':listgia[i][1],
                'name':listgia[i][0]
            }
        )for i in range(0, 10)
    ]
    
def tytrongKhoiLuong(df : pd.DataFrame):
    listKL: dict = dict(dict(df['listVol'].iloc[-1]))       
    listKL = sorted(listKL.items(), key=lambda x:x[1], reverse=True)
    return[
        (
            {
                'value':listKL[i][1],
                'name':listKL[i][0]
            }
        )for i in range(0, 10)
    ]
     
def tytrongFreeFloat(df : pd.DataFrame):
    listKL: dict = dict(dict(df['listVol'].iloc[-1]))       
    
    
    listGia: dict = dict(dict(df['listVol'].iloc[-1]))       
    
    listFF=  dict()
    for sym in List_HNX30:
        listFF.update({
            sym : round(float(listKL[sym]*listGia[sym]*dff[sym]/1000000.00),2)
        })
    listFF = sorted(listFF.items(), key=lambda x:x[1], reverse=True)
    return[
        (
            {
                'value':listFF[i][1],
                'name':listFF[i][0]
            }
        )for i in range(0, 10)
    ]
    
def changedatatable( tb: ui.table , data: list):
    tb.rows = data
    tb.update()
    
if __name__ in {"__main__", "__mp_main__"}:
    
    with ui.header(elevated=True):
        dark = ui.dark_mode()
        ui.button('Dark mode', on_click=dark.toggle).classes("absolute top-4 right-4")
        ui.select(options=List_HNX30, with_input=True, value= List_HNX30[0],
          on_change=lambda e:{ 
                        ch.update_table(table = e.value) , 
                        banggia.update_table ( table = e.value),
                        changedatatable(table_ch, infoSYM(e.value)),
                        changedatatable(table_bg, infoSYM2(e.value))
            }).props("dense outlined").classes("w-40 border-2 border-white-400 p-1 rounded-lg shadow-lg bg-gray-100")
    ### bieudo toan thi truong ###    
    df_thi_truong = pd.DataFrame(list(analysis_coll.find()))
    tt = echart_line_and_bar()
    tt.make_chart(chart_title= 'Tổng Quan Thị Trường HNX30', x_name='Time', left_y_name='Price', right_y_name='Volume')

    tt.add_trace(
        { 
            'name': 'Volume giao dịch',
            'data':tranferdata(df_thi_truong.loc[:,['timeServer', 'totalVol']]),
            'type': 'bar',
            'yAxisIndex': 1,
            'roundCap': True,
            
        }
    )
    tt.add_trace(
        {   
            'name': 'Điểm HNX30',
            'type': 'line',
            'yAxisIndex': 0,
            'data':tranferdata(df_thi_truong.loc[:,['timeServer', 'point']]),
        }
    )
        
    ### bieudo tron ###
    
    label1= ui.label('Tỷ Trọng Giá').style('position: absolute; top: 650px; left: 16%; padding: 10px; font-size: 24px;')
    label1= ui.label('Tỷ Trọng Khối Lượng').style('position: absolute; top: 650px; left: 44%; padding: 10px; font-size: 24px;')
    label1= ui.label('Tỷ Trọng Free-Float').style('position: absolute; top: 650px; left: 74%; padding: 10px; font-size: 24px;')
    
    pie = echart_pie()
    pie.make_chart()
        
    pie.add_trace({
        'name': 'Tỷ Trọng Giá',
        'type': 'pie',
        'radius': '50%',
        'center': ['20%', '50%'],
        'data': tytrongGia(df_thi_truong),
        'emphasis': {
            'itemStyle': {
                'shadowBlur': 10,
                'shadowOffsetX': 0,
                'shadowColor': 'rgba(0, 0, 0, 0.5)'
            }
        },
        
        
    },
    )
    
    pie.add_trace({
        'name': 'Tỷ Trọng Khối lượng',
        'type': 'pie',
        'radius': '50%',
        'center': ['50%', '50%'],
        'data': tytrongKhoiLuong(df_thi_truong),
        'emphasis': {
            'itemStyle': {
                'shadowBlur': 10,
                'shadowOffsetX': 0,
                'shadowColor': 'rgba(0, 0, 0, 0.5)'
            }
        }
    },
    )
    pie.add_trace({
        'name': 'Tỷ Trọng Free-Float',
        'type': 'pie',
        'radius': '50%',
        'center': ['80%', '50%'],
        'data':tytrongFreeFloat(df_thi_truong),
        'emphasis': {
            'itemStyle': {
                'shadowBlur': 10,
                'shadowOffsetX': 0,
                'shadowColor': 'rgba(0, 0, 0, 0.5)'
            }
        }
    },
    )
        
    ### bang gia tung ma chung khoan ###
   
    
    dataSYM =  pd.DataFrame(list(coll.find({'sym':  List_HNX30[0] , 'id': 3220})))
    with ui.row().classes('w-full'):
        with ui.card().style('height: 700px; width: 90%;'):
            ch = echart_line_and_bar()
            ch.make_chart(chart_title= 'Lượng giao dịch Mã Chứng Khoán CAP trong ngày', x_name='Time', left_y_name='Price', right_y_name='Volume')

            ch.add_trace(
                { 
                    'name': 'Stock Volume',
                    'data':tranferdata(dataSYM.loc[: ,  ['timeServer','lastVol']]),
                    'type': 'bar',
                    'yAxisIndex': 1,
                    'roundCap': True,
                    'markLine': {
                        'data': [{ 'type': 'average', 'name': 'Avg' }]
                    }
                }
            )
            ch.add_trace(
                {   
                    'name': 'Price',
                    'type': 'line',
                    'yAxisIndex': 0,
                    'data':tranferdata(dataSYM.loc[: ,  ['timeServer','lastPrice']]),
                    'markPoint': {
                        'data': [
                        { 'type': 'max', 'name': 'Max' },
                        { 'type': 'min', 'name': 'Min' },
                        ]
                    },
                    'markLine': {
                        'data': [{ 'type': 'average', 'name': 'Avg' }]
                    }
                }
            )
            ch.update_scale(yname='Price' , data=[48,52])
        
        with ui.card().style('height: 700px; width: 15%; position: absolute;right: 10px;'):
            ui.label('Thông tin Tổng hợp').style('font-size: 16px; font-weight: bold; text-align: center; margin-bottom: 16px;')
            table_ch = ui.table(columns=[
                {'name': 'info', 'label': 'Thông tin', 'field': 'info',  'align': 'left'},
                {'name': 'value', 'label': 'Giá trị', 'field': 'value', },
            ], rows= infoSYM(List_HNX30[0]), row_key='name').classes('w-full')
    
    #### chart bang gia
    
    with ui.row().classes('w-full'):
        with ui.card().style('height: 700px; width: 90%;'):
            dataBangGia =  pd.DataFrame(list(coll.find({'sym':  List_HNX30[0] , 'id': 3210})))
            banggia = echart_bang_gia()
            banggia.make_chart(chart_title= 'Bảng Giá Mã Chứng Khoán CAP trong ngày', x_name='Price', left_y_name='Volume')
            banggia.add_trace(
                { 
                    'name': 'Buy Volume',
                    'data':tranferdatabanggia_B(dataBangGia),
                    'type': 'bar',
                    'roundCap': True,
                    'markLine': {
                        'data': [
                            { 'type': 'max', 'name': 'Max' },
                        ]
                    },
                    'stack' : 'ssvol',
                    'itemStyle': {'color': 'green'},
                }
                
            )
            
            banggia.add_trace(
                { 
                    'name': 'Sell Volume',
                    'data':tranferdatabanggia_S(dataBangGia),
                    'type': 'bar',
                    'roundCap': True,
                    'markLine': {
                        'data': [
                            { 'type': 'min', 'name': 'Min' }
                        ]
                    },
                    'stack' : 'ssvol',
                    'itemStyle': {'color': 'red'},
                    
                }
            )
        with ui.card().style('height: 700px; width: 15%; position: absolute;right: 10px;'):
            ui.label('Thông tin Tổng hợp').style('font-size: 16px; font-weight: bold; text-align: center; margin-bottom: 16px;')
            table_bg = ui.table(columns=[
                {'name': 'info', 'label': 'Thông tin', 'field': 'info',  'align': 'left'},
                {'name': 'value', 'label': 'Giá trị', 'field': 'value', },
            ], rows= infoSYM2(List_HNX30[0]), row_key='name').classes('w-full')
            
    
    ui.run(
        host='localhost',
        port=8080
    )