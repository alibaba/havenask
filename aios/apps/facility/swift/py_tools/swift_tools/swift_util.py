#!/bin/env python
import os
import time

class SwiftUtil:
    @classmethod
    def protoEnumToString(self, parent, itemName):
        for (fieldDescriptor, fieldValue) in parent.ListFields():
            if fieldDescriptor.name == itemName:
                return fieldDescriptor.enum_type.values_by_number[fieldValue].name
        return "UNKNOWN"

    @classmethod
    def currentTime(self):
        #description Return the time as a floating point number expressed 
        #in seconds since the epoch, in UTC
        return time.time()

    @classmethod
    def convertStr2Time(self, timeStr, timeFormat):
        return time.mktime(time.strptime(timeStr, timeFormat))

    @classmethod
    def convertTime2Str(self, timeSec, timeFormat):
        return time.strftime(timeFormat,
                             time.localtime(timeSec))
    
    @classmethod
    def parseIPList(self, ipStr, sep=";"):
        if not ipStr:
            return []
        ipStrList = ipStr.split(sep)
        ipList = []
        for ipStr in ipStrList:
            ipStr = ipStr.strip()
            if len(ipStr) == 0:
                continue
            if self.checkIpAddress(ipStr):
                ipList.append(ipStr)
        return ipList


    @classmethod
    def checkIpAddress(self, ipStr):
        if not ipStr:
            return False
        ipStr = ipStr.strip()
        lis = ipStr.split(".")
        if len(lis) != 4:
            return False
        for item in lis:
            ret, num = self.changeStr2Int(item)
            if not ret:
                return False
            if num < 0 or num > 255:
                return False
        return True

    @classmethod
    def changeStr2Int(self, string):
        num = 0
        try:
            num = int(string)
        except Exception, e:
            return False, None
        return True, num

    @classmethod
    def splitStr(self, string, separater=";", 
                 saveSep=False, stripItem=True):
        if not string:
            return []
        lis = []
        for item in string.split(separater):
            if stripItem:
                item = item.strip()
            if item:
                if saveSep:
                    item = item + separater
                lis.append(item)
        return lis

    @classmethod
    def splitStrNotDelSep(self, string, separater=";"):
        if not string:
            return []
        lis = []
        pos = string.find(separater)
        start = 0
        while pos != -1:
            end = pos + len(separater)
            item = string[start:end]
            item = item.strip()
            if item:
                lis.append(item)          
            start = end 
            pos = string.find(separater, end)
        item = string[start:]
        if item:
            lis.append(string[start:])
        return lis
    
    @classmethod
    def getUniqueList(self, lis):
        if type(lis) != type([]):
            return None
        
        checked = [] 
        for item in lis: 
            if item not in checked: 
                checked.append(item) 
        return checked

    @classmethod
    def getServerSpecs(self, ipAdds, ports):
        if not ipAdds or not ports:
            return None

        ipAddList = ipAdds
        if type(ipAdds) != type([]):
            ipAddList = [ipAdds]

        cnt = len(ipAddList)
        portList = []
        if type(ports) != type([]):
            for i in range(0, cnt):
                portList.append(ports)
        else:
            portList = ports

        if len(ipAddList) != len(portList):
            return None
            
        specList = []
        for i in range(0, cnt):
            spec = "tcp:%s:%s"% (ipAddList[i].strip(), portList[i].strip())
            specList.append(spec)

        return specList

        
        
