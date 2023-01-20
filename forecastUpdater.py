import pandas as pd
import numpy
from datetime import datetime,timedelta
import os
from pymongo import MongoClient,UpdateMany,UpdateOne
import mysql.connector
import json
import time
import numpy as np

def forecastUpdater(directoryPath,groupId,columnList,Model):
    print(groupId+">"+model+" Verileri Güncelleniyor")


    fileList=os.listdir(directoryPath)

    kontrolTarih=datetime.now()-timedelta(days=7)

    for gunSay in range(0,8):

        kontrolStr=datetime.strftime(kontrolTarih+timedelta(days=gunSay),"%Y%m%d")


        ty="tsaçmdaçs"
     

        for fileName in fileList:
            if fileName.__contains__(".csv")==False:

                continue

            if fileName.__contains__(kontrolStr)==True:

                dosyaVeriDF=pd.read_csv(directoryPath+"\\"+fileName,sep=';')



                rowArr=[]
                
                myclient = MongoClient("mongodb://89.252.157.127:27017/")
                        
                mydbMongoDB = myclient["bigVentusDataDB"] #db
  
                mongoCol= mydbMongoDB["siteForecast_"+groupId]
    

                rowArr=[]

                for rowSay in range(0,dosyaVeriDF.shape[0]):

                    if str(dosyaVeriDF.iloc[rowSay][0]).__contains__("Total")==True:
                        continue

                    if str(dosyaVeriDF.iloc[rowSay][0]).__contains__("The localized")==True:
                        continue

                    initDate=pd.to_datetime(datetime.strftime(kontrolTarih+timedelta(days=gunSay),"%Y-%m-%d"))

                    initTime="0"

                    if fileName.__contains__("_0900"):
                        initTime="6"
                    elif fileName.__contains__("_1500"):
                        initTime="12"
                    elif fileName.__contains__("_2100"):
                        initTime="18"

                    dictColumn={}

                    for columnCount in range(0,len(columnList)):

                        dictColumn[columnList[columnCount]]=dosyaVeriDF.iloc[rowSay][columnCount]


                    dictColumn["initDate"]=initDate
                    dictColumn["initTime"]=initTime
                    dictColumn["Model"]=Model

                    rowArr.append(UpdateMany({"pStartTime":dosyaVeriDF.iloc[rowSay][0],"pEndTime":dosyaVeriDF.iloc[rowSay][1],"initTime":initTime,"initDate":initDate,"Model":Model},{ "$set": dictColumn},upsert=True))

                mongoCol.bulk_write(rowArr)  
                
                mongoCol.delete_one({"initDate":datetime.now()-timedelta(days=11)})

                mongoCol.create_index("initDate",unique=False)
                    
                mongoCol.create_index("initTime",unique=False)

                myclient.close()
    
    print(groupId+">"+model+" Verileri Güncellendi")

               
if __name__=="__main__":
    



    while 2>1:



        anaKlasor="Z:\\"
        
        with open("config.json","r") as file:

            dbApiInfo=json.load(file)

        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
        modelList=["Model1","Model2","MOS"]

        cursor=myDBConnect.cursor()

        selectTxt="Select id,siteTypeId,matlabId From siteList where matlabId>0"
        
        cursor.execute(selectTxt)
        
        siteTable = cursor.fetchall()

        myDBConnect.close()
        
        

        siteTableDF=pd.DataFrame(siteTable,columns=["siteId","siteTypeId","matlabId"])

        modelList=["Model1","Model2","MOS"]

        for siteSay in range(0,siteTableDF.shape[0]):

            matId=str(siteTableDF.iloc[siteSay][2])

            altKlasor="Wind"

            if siteTableDF.iloc[siteSay][1]==2:

                altKlasor="Solar"

            if siteTableDF.iloc[siteSay][1]==4:

                altKlasor="Meteo"
            

            for model in modelList:

                if os.path.exists(anaKlasor+altKlasor+"\\"+matId+"\\"+model)==True:

                    paramList=[]

                    if siteTableDF.iloc[siteSay][1]==4:

                        paramList=["pStartTime","pEndTime","WindSpeed","Gust_Speed","Temperature ","Relative_Humidity","Hourly_Precipitation","Global_Horizontal_Insolation","Cloudiness Index"]  

                    else:
                        
                        paramList=["pStartTime","pEndTime","Production"]

                    
                    forecastUpdater(anaKlasor+altKlasor+"\\"+matId+"\\"+model,str(siteTableDF.iloc[siteSay][0]),paramList,model)


        myDBConnect.connect()

        cursor=myDBConnect.cursor()

        selectTxt="Select id,groupId From siteGroups"
        
        cursor.execute(selectTxt)
        
        groupTable = cursor.fetchall()

        myDBConnect.close()

        altKlasor="Groups"

        paramList=["pStartTime","pEndTime","Production"]

        for group in groupTable:

            for model in modelList:
                if os.path.exists(anaKlasor+altKlasor+"\\"+str(group[0])+"\\"+model)==True:


                    forecastUpdater(anaKlasor+altKlasor+"\\"+str(group[0])+"\\"+model,str(group[1]),paramList,model)
        
       
        bekle=datetime.now()+timedelta(hours=1)

        while (datetime.now()-bekle).total_seconds()<0:

            print("Sonraki Çalışmaya Kalan Süre:"+str(np.round((bekle-datetime.now()).total_seconds()/60,0))+" DK","\r")

            time.sleep(5)           

    