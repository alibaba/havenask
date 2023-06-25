#!/bin/env python

import re
import sys
import os
import socket
from optparse import OptionParser
import tempfile
import subprocess
import json
import time
import local_search_starter
import re

class LocalSearchUpdateCmd(local_search_starter.LocalSearchStartCmd):
    '''
local_search_update.py
    {-i index_dir           | --index=index_dir}
    {-c config_dir          | --config=config_dir}
    {-p port_start          | --prot=prot_start}
    {-z zone_name           | --zone=zone_name}
    {-b binary_path         | --binary=binary_path}

options:
    -i index_dir,     --index=index_dir              : required, query string
    -c config_dir,    --config=config_dir            : required, qrs http/tcp address,
    -p port_start,    --prot=port_start              : optional, http port, arpc port is start +1 (total port may use start + n*3 ) [default 12000].
    -p zone_name,     --zone=zone_name               : optional, special zone to start
    -b binary_path,   --binary=binary_path           : optional, special binary path to load

examples:
    ./local_search_update.py -i /path/to/index -c path/to/config 
    ./local_search_update.py -i /path/to/index -c path/to/config -p 12345
    '''

    def __init__(self):
        super(LocalSearchUpdateCmd, self).__init__()

    def usage(self):
        print self.__doc__ % {'prog' : sys.argv[0]}

    def addOptions(self):
        super(LocalSearchUpdateCmd, self).addOptions()

    def parseParams(self, optionList):
        return super(LocalSearchUpdateCmd, self).parseParams(optionList)

    def checkOptionsValidity(self, options):
        return super(LocalSearchUpdateCmd, self).checkOptionsValidity(options)

    def initMember(self, options):
        return super(LocalSearchUpdateCmd, self).initMember(options)

    def run(self):
        if not self._getPortListArray():
            errmsg= "get port listArray failed"
            print errmsg
            return ("", errmsg, -1)
        if not self._updateSearchConfig():
            errmsg= "update search failed"
            print errmsg
            return ("", errmsg, -1)
        if not self._updateQrsConfig():
            errmsg= "update qrs failed"
            print errmsg
            return ("", errmsg, -1)
        print "update success"
        return ("", "", 0)

    def _getPortListArray(self):
        try:
            with open(self.qrsPortFile, "r") as f:
                portdict = json.load(f)
                self.qrs_port_list = [portdict["http_arpc_port"], portdict["arpc_port"], portdict["grpc_port"]]
            
            with open(self.searcherPortFile, "r") as f:
                portjson = json.load(f)
                self.searcher_port_list = []
                for portinfo in portjson:
                    item = local_search_starter.PortListItem()
                    item.role = portinfo[0]
                    portdict = portinfo[1]
                    item.ports = [portdict["http_arpc_port"], portdict["arpc_port"], portdict["grpc_port"]]
                    self.searcher_port_list.append(item)
        except Exception as e:
            print(str(e))
            return 0
        return 1
    
    def _updateSearchConfig(self):
        zoneNames = self._getNeedStartZoneName()
        tableInfos = self._genTargetInfos(zoneNames, 1)
        if not self._loadSearcherTarget(tableInfos):
            return False
        return True

    def _updateQrsConfig(self):
        if not self._loadQrsTarget():
            return False
        return True
    

    def _loadQrsTarget(self, timeout = 300):
        localTopoStr = "##".join(self.readyZones.values())
        target = {
            "service_info" : {
                "cm2_config" : {
                    "local" : localTopoStr
                },
                "part_count" : 0,
                "part_id" : 0,
                "zone_name": "qrs"
            },
            "biz_info" : {
                "default" : {
                    "config_path" : self.onlineConfigPath
                }
            },
            "table_info" : {
            },
            "clean_disk" : False
        }
        targetStr = json.dumps(target)
        requestSig = targetStr
        globalInfo = {"customInfo":targetStr}
        targetRequest = { "signature" : requestSig,
                          "customInfo" : targetStr,
                          "globalCustomInfo": json.dumps(globalInfo)
        }
        portList = self.getQrsPortList()
        httpArpcPort = portList[0]
        arpcPort = portList[1]
        address = "%s:%d" %(self.ip, httpArpcPort)
        while timeout > 0:
            time.sleep(5)
            timeout -= 5
            retCode, out, err, _ = self.curl(address, "/HeartbeatService/heartbeat", targetRequest)
            if retCode != 0:
                print "set qrs target %s failed." % targetStr
                continue
            response = json.loads(out)
            if response["signature"] == requestSig:
                print "qrs is ready for search, http port %s, arpc port %s" % (httpArpcPort, arpcPort)
                return True
        return timeout > 0
    
    def _loadSearcherTarget(self, targetInfos, timeout = 300):
        self.readyZones = {}
        targetInfos = targetInfos[0]
        while timeout > 0:
            time.sleep(5)
            timeout -= 5
            count = 0
            for targetInfo in targetInfos:
                portList = self._getSearcherPortList(count)
                count += 1
                zoneName = targetInfo[0]
                partId = targetInfo[1]
                roleName = zoneName + "_" + str(partId)
                if self.readyZones.has_key(roleName) :
                    continue
                target = targetInfo[3]
                httpArpcPort = portList[0]
                arpcPort = portList[1]
                targetStr = json.dumps(target)
                requestSig = targetStr
                globalInfo = {"customInfo":targetStr}
                targetRequest = { "signature" : requestSig,
                                  "customInfo" :targetStr,
                                  "globalCustomInfo": json.dumps(globalInfo)
                }
                address = "%s:%d" %(self.ip, httpArpcPort)
                retCode, out, err, _ = self.curl(address, "/HeartbeatService/heartbeat", targetRequest)
                if retCode != 0:
                    print "set target %s failed." % targetStr
                    print "curl HeartbeatService with target %s" %(targetRequest)
                    print "get output %s error %s" %(out, err)
                    continue
                response = json.loads(out)
                if response["signature"] == requestSig:
                    serviceInfo = json.loads(response["serviceInfo"])
                    arpcAddress = "%s:%d" %(self.ip, arpcPort)
                    topoStr = serviceInfo["cm2"]["topo_info"]
                    targetTopoStr = "tcp:" + arpcAddress + "#" +topoStr
                    self.readyZones[roleName] = targetTopoStr
                    print "searcher [%s] is ready for search, topo [%s]" % (roleName, targetTopoStr)
                else:
                    print("request signature %s" % requestSig)
                    print("response signature %s" % response["signature"])
                
                if len(targetInfos) == len(self.readyZones):
                    print "all searcher is ready."
                    return True
        return timeout > 0

if __name__ == '__main__':
    cmd = LocalSearchUpdateCmd()
    if len(sys.argv) < 3:
        cmd.usage()
        sys.exit(-1)
    if not cmd.parseParams(sys.argv):
        cmd.usage()
        sys.exit(-1)
    data, error, code = cmd.run()
    if code != 0:
        if error:
            print error
        sys.exit(code)
    sys.exit(0)
