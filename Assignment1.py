import json
from mpi4py import MPI
import numpy as np
import collections
import time
start_time = time.time()
#initialize mpi 
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
#data = []
"""This part parse the grid data into a dictionary which has a range of coordinates for each gridID"""
#gridFile = 'C:/Users/Christian Lan/OneDrive/COMP90024 Clust and Cloud Computing/Assignment1/melbGrid.json'
gridFile = '/data/projects/COMP90024/melbGrid.json'
with open(gridFile) as f:
    #for line in f:
     #  data.append(json.loads(line))
#jsonData =open('C:/Users/Christian Lan/Desktop/COMP90024 Clust and Cloud Computing/Assignment1/tinyTwitter.json','rU','utf-8')
    data = json.load(f)


gridData = []

for features in data["features"]:
    girdDict = {}
    girdDict["gridId"] = features["properties"]["id"]
    girdDict["xmax"] = features["properties"]["xmax"]
    girdDict["xmin"] = features["properties"]["xmin"]
    girdDict["ymin"] = features["properties"]["ymin"]
    girdDict["ymax"] = features["properties"]["ymax"]
    gridData.append(girdDict)
#print gridData





#Initilize two lists to store json data, and count is used for testing the first line
coordData = []
count = 0
tweetData = []

#tweetFile = 'C:/Users/Christian Lan/OneDrive/COMP90024 Clust and Cloud Computing/Assignment1/Untitled-1.json'
tweetFile = '/data/projects/COMP90024/bigTwitter.json'

"""only master node parse the coordinates from each tweet data. Store the first line and for each line add them
    together and also add ']}' at the end so that for each line the format of json keep consistent"""
if rank ==0:
    with open(tweetFile,'r', encoding='UTF-8') as g:
        
        for line in g:
            count += 1
            if count ==1:
                firstLine = line
            elif line.startswith("]}"):
        
                continue
            
            else:
                #If a line not endswith ,\n, this means the current line is last json data. 
                #Same parse method but with different endings to keep the data type consistent
                if not line.endswith(",\n"):
                    
                    coordData = json.loads(firstLine+line+"]}")
                    for row in coordData["rows"]:
                        
                        tweetDict = {}
                        #Here checking if a tweet missing coordinates for both fields, if so, skip the line
                        if row["doc"]["coordinates"]:
                            singleCoord = row["doc"]["coordinates"]["coordinates"]
                        else:
                            if row["doc"]["geo"]:
                                reverseCoord = row["doc"]["geo"]["coordinates"]
                                singleCoord = reverseCoord.reverse()
                            else:
                                continue
                        #singleCoord = row["value"]["geometry"]["coordinates"] This format is testing for tiny data set
                        tweetDict["coord"] = singleCoord


                        rawText = row["doc"]["text"]
                        #rawText = row["value"]["properties"]["text"]
                        hastags = rawText.split(" ")[1:-1]
                        for hashtag in hastags:
                            if hashtag.startswith("#"):
                            
                                tweetDict["hashtag"] = hashtag.lower()
                        tweetData.append(tweetDict)
                   
                    
                    continue
                    
                #This part doing the regular parse job
                coordData = json.loads(firstLine+line[0:len(line)-2]+"]}")
                for row in coordData["rows"]:
                    tweetDict = {}
                    if row["doc"]["coordinates"]:
                        singleCoord = row["doc"]["coordinates"]["coordinates"]
                    else:
                        if row["doc"]["geo"]:
                            reverseCoord = row["doc"]["geo"]["coordinates"]
                            singleCoord = reverseCoord.reverse()
                        else:
                            continue
                    
                    tweetDict["coord"] = singleCoord

                    rawText = row["doc"]["text"]
                    hastags = rawText.split(" ")[1:-1]
                    for hashtag in hastags:
                        if hashtag.startswith("#"):
                            
                            tweetDict["hashtag"] = hashtag.lower()
                    tweetData.append(tweetDict)

            
    #If running in more than one core or nodes, split data into sub-arrays by # of nodes
    if size>1:
        segmentData = np.array_split(tweetData,size)
    #print("rank",rank)
    #print("size",size)
else:
    segmentData = None
#If parallize tasks, scatter the data 
if size >1:
    parallelData = comm.scatter(segmentData,root = 0)
else:
    parallelData = tweetData

                
#print("paralleData",parallelData)



gridCount = {}

"""Doing the calculation for each coord. Count the number of posts for each grid cell and 
    storing the hastags for each grid"""
for coord in parallelData:
    appendCoord = False
    
    for grid in gridData:
        #print grid
        if coord["coord"][0] > grid["xmin"] and coord["coord"][0] <= grid["xmax"]:
            if coord["coord"][1] >= grid["ymin"] and coord["coord"][1] < grid["ymax"]:
                gridHashData = []
                gridHash = {}
                if grid["gridId"] not in gridCount:
                    gridCount[grid["gridId"]] = {}
                    if "count" not in gridCount[grid["gridId"]]:
                        gridCount[grid["gridId"]]["count"] =1

                    else:
                        gridCount[grid["gridId"]]["count"] +=1
                    if "hashtag" in coord:
                        if "hashtags" not in gridCount[grid["gridId"]]:
                            
                            gridCount[grid["gridId"]]["hashtags"] =  [coord["hashtag"]]
                        else:
                            gridCount[grid["gridId"]]["hashtags"].append(coord["hashtag"])
                else:
                    if "count" not in gridCount[grid["gridId"]]:
                        gridCount[grid["gridId"]]["count"] =1

                    else:
                        gridCount[grid["gridId"]]["count"] +=1
                    if "hashtag" in coord:
                        if "hashtags" not in gridCount[grid["gridId"]]:
                            
                            gridCount[grid["gridId"]]["hashtags"] =  [coord["hashtag"]]
                        else:
                            gridCount[grid["gridId"]]["hashtags"].append(coord["hashtag"])


                    
                
gatheredGridData = {}
gridCount = comm.gather(gridCount,root=0)
#gatheredGridData = comm.gather(gridCount,root=0)
#print("gridCount after Gather",gridCount)
#print(gridCount)
#print("")
"""After gathering the data from differnt rank, the master node calculate the results"""
if rank == 0:
    for result in gridCount:
        for grid in gridData:
            if grid["gridId"] in result:
                if grid["gridId"] not in gatheredGridData:
                    gatheredGridData[grid["gridId"]] = {}



                    if "count" not in gatheredGridData[grid["gridId"]]:
                        gatheredGridData[grid["gridId"]]["count"] = result[grid["gridId"]]["count"]
                    else:
                        gatheredGridData[grid["gridId"]]["count"] += result[grid["gridId"]]["count"]
                    if "hashtags" in result[grid["gridId"]]:
                            if "hashtags" not in gatheredGridData[grid["gridId"]]:
                                
                                gatheredGridData[grid["gridId"]]["hashtags"] =  result[grid["gridId"]]["hashtags"]
                            else:
                                gatheredGridData[grid["gridId"]]["hashtags"] += (result[grid["gridId"]]["hashtags"])
                else:
                    if "count" not in gatheredGridData[grid["gridId"]]:
                        gatheredGridData[grid["gridId"]]["count"] = result[grid["gridId"]]["count"]
                    else:
                        gatheredGridData[grid["gridId"]]["count"] += result[grid["gridId"]]["count"]
                    if "hashtags" in result[grid["gridId"]]:
                            #print("lol")
                            if "hashtags" not in gatheredGridData[grid["gridId"]]:
                                
                                gatheredGridData[grid["gridId"]]["hashtags"] =  result[grid["gridId"]]["hashtags"]
                            else:
                                gatheredGridData[grid["gridId"]]["hashtags"] += (result[grid["gridId"]]["hashtags"])
    #print(gatheredGridData) 

"""ranking the grid boxes and generating the results"""
    postRankingList = []
    print("Top 5 hashtags for each Grid boxes:")
    for grid in gridData:
        #print(grid["gridId"])
        #print(gridCount)
        if grid["gridId"] in gatheredGridData:
            #print("testing1")
            
            
            if "hashtags" in gatheredGridData[grid["gridId"]]:
                #print("testing")
                rankings = collections.Counter(gatheredGridData[grid["gridId"]]["hashtags"]).most_common()
                #print(len(rankings))
                count = 0
                result = []
                if len(rankings) ==1:
                    result.append(rankings[0])
                else:
                    for i in range(0,len(rankings)-1):

                        #print("haha",rankings[i][1])
                        if count == 5:
                            break
                        else:
                            if rankings[i][1] != rankings[i+1][1]:
                                result.append(rankings[i])
                                count +=1
                            else:
                                result.append(rankings[i])


            
                print(grid["gridId"],result)
                #print(len(result))
                print("")
                #print("")
                postRankingList.append([grid["gridId"],gatheredGridData[grid["gridId"]]["count"]])
                #print(postRankingList)
                #print(gridCount)
            

    """for cellData in gridCount:
        for grid in gridData:
            if cellData[grid["gridId"]]["count"] >"""

    rankedList = sorted(postRankingList, key = lambda x: x[1], reverse = True  )
    print("Ranking of the Grid boxes based on tweet posts:")
    print(rankedList)
    print("")
    totalTime = time.time() - start_time
    print("Total running time:",totalTime," s")
    #print(tweetData)
    #print(sorted(gridCount[grid["gridId"]]["count"] for grid in gridData if grid["gridId"] in gridCount))
                    

            
                

        #print"Final", coordData
            #print coordData["value"]["geometry"]["coordinates"]
            #print coordData["doc"]["entities"]["hashtags"]
