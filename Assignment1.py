import json
from mpi4py import MPI
import numpy as np
import collections
import time
start_time = time.time()

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
"""Part 1:
    Every process parse the data from the melbGrid file. store the grid data into a dictionary which has
    each grid cell's ID and their range of coordinates."""
#data = []
#gridFile = 'C:/Users/Christian Lan/OneDrive/COMP90024 Clust and Cloud Computing/Assignment1/melbGrid.json'
gridFile = '/data/projects/COMP90024/melbGrid.json'
with open(gridFile) as f:
   
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





"""Part 2: 
    Parallel parse the bigTwitter file by reading each line. If the current line belongs to current rank,
    parse the data and then store the coordinates and hashtage of each tweet. Then append them into a list"""
coordData = []
count = 0


tweetData = []

#tweetFile = 'C:/Users/Christian Lan/OneDrive/COMP90024 Clust and Cloud Computing/Assignment1/Untitled-1.json'
tweetFile = '/data/projects/COMP90024/bigTwitter.json'

with open(tweetFile,'r', encoding='UTF-8') as g:
    
    for line in g:
        count += 1
        if count ==1:
            firstLine = line
        elif line.startswith("]}"):
    
            continue
        
        else:
            if size > 1:
                dontSkip = False
            else:
                dontSkip = True
            if rank == (count+1)%size or dontSkip:

                if not line.endswith(",\n"):
                    #print ""
                    #print(" ", line)
                    coordData = json.loads(firstLine+line+"]}")
                    for row in coordData["rows"]:
                        #for big [doc][coordinates][coordinates]
                        tweetDict = {}
                        #singleCoord = row["doc"]["coordinates"]["coordinates"]
                        if row["doc"]["coordinates"]:
                            singleCoord = row["doc"]["coordinates"]["coordinates"]
                        else:
                            if row["doc"]["geo"]:
                                reverseCoord = row["doc"]["geo"]["coordinates"]
                                singleCoord = reverseCoord.reverse()
                            else:
                                continue
                        #singleCoord = row["value"]["geometry"]["coordinates"]
                        tweetDict["coord"] = singleCoord


                        rawText = row["doc"]["text"]
                        #rawText = row["value"]["properties"]["text"]
                        hastags = rawText.split(" ")[1:-1]
                        for hashtag in hastags:
                            if hashtag.startswith("#"):
                            
                                tweetDict["hashtag"] = hashtag.lower()
                        tweetData.append(tweetDict)
                        #print(rawText)
                    #print singleCoord
                    
                    continue
                    

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

            

                
"""Parallel calculate that the cell of the posts belong to and store the number of posts and hastages for this grid cell"""
parallelData = tweetData


gridCount = {}


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


                    
"""Part3:
    Master node gather the data from all the process and rank the data
    Since the results from other rank is a list. First parse the list and get the ideal 
    dictionary datatype from it then process the data.
"""                
gatheredGridData = {}
gridCount = comm.gather(gridCount,root=0)

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
