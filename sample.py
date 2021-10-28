import pandas as pd
data = pd.read_csv('미분배로그/공연미분배로그/pf_log_201101.txt', engine="python", encoding="UTF-8" , sep="\t", header= None)
# data = pd.read_csv("미분배로그/공연미분배로그/*.txt", engine="python", encoding="UTF-8" , sep="\t", header= None)
import os

path = "미분배로그/공연미분배로그/"
files = os.listdir("미분배로그/공연미분배로그")
data=pd.DataFrame()
## " 를 어떻게 해결할것인가
for file in files:
    data=pd.concat([data,pd.read_csv(path+file,  engine="python", encoding="UTF-8" , sep="\t", header= None, quotechar='"', doublequote=False)], ignore_index=True)
# columns = ['1','2','3','4','album','title','singer','8','9','10','11']
# data = data.iloc[:,[4,5,6]]
# data.columns = ['album', 'title', 'singer']
