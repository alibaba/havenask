import urllib
import time
import sys
import os
import json_wrapper

kmon_url = 'http://metric.alibaba-inc.com/api/query?token=c5a2a17e'
metrics = ['swift.partition.read.request.readRequestQps', 'swift.partition.write.request.writeRequestQps','swift.partition.other.maxMsgId']
if __name__ == '__main__':
    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage:", sys.argv[0], "<topic_name_file> <instance> <begin time>"
        sys.exit(1)
    topicFile = open(sys.argv[1])
    instance = sys.argv[2]
    begTime = time.time()*1000 - (int)(sys.argv[3]) *3600*1000 -24*3600*1000
    res = {}
    for topicName in topicFile.readlines():
        tokens = topicName.split(' ')
        lastWriteTime = tokens[2]
        if time.time() - int(lastWriteTime) < (int)(sys.argv[3])*3600 :
            continue
        topicName = tokens[0]
        params ='{"start":%d,"queries":[{"metric":"swift.partition.write.request.writeRequestQps","tags":{"instance":"%s","topic":"%s"},"aggregator":"sum"},{"metric":"swift.partition.read.request.readRequestQps","tags":{"instance":"%s","topic":"%s"},"aggregator":"sum"}]}' %(begTime, instance, topicName, instance, topicName)
        #print params
        f = urllib.urlopen(kmon_url, params)
        resStr = f.read()
        #print resStr
        resJson = json_wrapper.read(resStr)
        #print resJson
        for i in range(0,len(resJson)):
            metrics = resJson[i]['metric']
            dps = resJson[i]['dps']
            lastKey =''
            lastVal = -11111
            items = dps.items()
            items.sort()
            #print items
            for (key, value) in items:
                if lastVal != value:
                    lastKey = key
                    lastVal = value
            h = (time.time() - (int)(lastKey))/3600
            #print topicName, metrics, lastKey, h, lastVal
            if h > int(sys.argv[3]) :
                print topicName, metrics, lastKey, h, lastVal
                


