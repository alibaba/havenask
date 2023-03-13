#!/bin/env python

import re
import sys
import os
import copy
import socket
from optparse import OptionParser
import tempfile
import subprocess
import json
import time
import socket
import shutil
import httplib
from datetime import datetime
hape_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../../../"))
sys.path.append(hape_path)
from hape.utils.partition import get_partition_intervals


class PartSearchStartCmd(object):
    '''
local_search_starter.py
    {-i index_dir           | --index=index_dir}
    {-c config_dir          | --config=config_dir}
    {-p port_start          | --prot=prot_start}
    {-z zone_name           | --zone=zone_name}
    {-b binary_path         | --binary=binary_path}
    {-t timeout             | --timeout=timeout}
    {-l preload             | --preload=preload}
    {-g agg_name            | --agg=agg_name}
    {-w para_search_ways    | --para_ways=para_search_ways}
    {-S disable_sql         | --disableSql=sql_flag}
    {-W disable_sql_warmup  | --disableSqlWarmup=sql_warmup_flag}
    {-m multi_biz           | --multiBiz=multi_biz}

options:
    -i index_dir,     --index=index_dir              : required, index dir
    -c config_dir,    --config=config_dir            : required, config path,
    -p port_list,     --port=port_list               : optional, port list, http port is first port, arpc port is second port, default 12000, if only one port is designed, next port is start +1 (total port may use start + n*3 )
    -z zone_name,     --zone=zone_name               : optional, special zone to start
    -j auxiliary_table, --tables=auxiliary_table     : optional, special auxiliary table to load
    -b binary_path,   --binary=binary_path           : optional, special binary path to load
    -t timeout,       --timeout=timeout              : optional, special timeout load [defalut 300]
    -l preload,       --preload=preload              : optional, special lib to load
    -s serviceName,   --serviceName=serviceName      : optional, serviceName [default ha3_suez_local_search]
    -a amonPort,      --amonPort=amonPort            : optional, amon port [default 10086]
    -g agg_name,      --aggName=aggName              : optional, aggName [default default_agg_4]
    --basicTuringBizNames=names                      : optional, common turing biz names, example [biz1,biz2]
    --basicTuringPrefix=prefix                       : optional, common turing request http prefix, example [/common]
    -w para_search_ways, --paraSearchWays=paraSearchWays  : optional, paraSearchWays [default 2,4]
    -S disable_sql,   --disableSql=true              : optional, disableSql [default false]
    -W disable_sql_warmup, --disableSqlWarmup=true   : optional, disableSqlWarmup [default false]
    -d dailyrun mode, --dailyrunMode=true            : optional, enable dailyrun mode
    -m multi_biz,        --multiBiz=multiBiz         : optional, enable multi_biz

examples:
    ./local_search_starter.py -i /path/to/index -c path/to/config
    ./local_search_starter.py -i /path/to/index -c path/to/config -p 12345
    ./local_search_starter.py -i /path/to/index -c path/to/config -p 12345,22345,32345
    ./local_search_starter.py -i /path/to/index -c path/to/config -z zone1,zone2 -j table1:t1,t3;table2:t3
    '''

    def __init__(self):
        self.parser = OptionParser(usage=self.__doc__)

    def usage(self):
        print self.__doc__ % {'prog' : sys.argv[0]}

    def addOptions(self):
        self.parser.add_option('', '--role', action='store', dest='role')
        self.parser.add_option('', '--part_id', action='store', dest='partitionIndex', type='int')
        self.parser.add_option('', '--part_count', action='store', dest='partitionCount', type='int')
        self.parser.add_option('', '--label', action='store', dest='label', type='str')
        
        self.parser.add_option('-i', '--index', action='store', dest='indexPath')
        self.parser.add_option('-c', '--config', action='store', dest='configPath')
        self.parser.add_option('-p', '--port', action='store', dest='portList')
        self.parser.add_option('', '--qrsHttpArpcBindPort', dest='qrsHttpArpcBindPort', type='int', default=0)
        self.parser.add_option('', '--qrsArpcBindPort', dest='qrsArpcBindPort', type='int', default=0)

        self.parser.add_option('-z', '--zone', action='store', dest='zoneName')
        self.parser.add_option('-j', '--tables', action='store', dest='atables')
        self.parser.add_option('-b', '--binary', action='store', dest='binaryPath')
        self.parser.add_option('-t', '--timeout', action='store', dest='timeout',type='int', default=300)
        self.parser.add_option('-l', '--preload', action='store', dest='preload')
        self.parser.add_option('-s', '--serviceName', action='store', dest='serviceName', default="ha3_suez_local_search")
        self.parser.add_option('-a', '--amonPort', action='store', dest='amonPort',type='int', default=10086)
        self.parser.add_option('-g', '--aggName', action='store', dest='aggName')
        self.parser.add_option('', '--basicTuringBizNames', action='store', dest='basicTuringBizNames')
        self.parser.add_option('', '--basicTuringPrefix', action='store', dest='basicTuringPrefix')
        self.parser.add_option('-w', '--paraSearchWays', action='store', dest='paraSearchWays')
        self.parser.add_option('-S', '--disableSql', action='store_true', dest='disableSql', default=False)
        self.parser.add_option('-W', '--disableSqlWarmup', action='store_true', dest='disableSqlWarmup', default=False)
        self.parser.add_option('', '--qrsExtraQueue', action='store', dest='qrsQueue')
        self.parser.add_option('', '--searcherExtraQueue', action='store', dest='searcherQueue')
        self.parser.add_option('', '--searcherThreadNum', action='store', dest='searcherThreadNum', type='int')
        self.parser.add_option('', '--searcherQueueSize', action='store', dest='searcherQueueSize', type='int')
        self.parser.add_option('', '--qrsThreadNum', action='store', dest='qrsThreadNum', type='int')
        self.parser.add_option('', '--qrsQueueSize', action='store', dest='qrsQueueSize', type='int')
        self.parser.add_option('-d', '--dailyrunMode', action='store_true', dest='dailyrunMode', default=False)
        self.parser.add_option('-m', '--multiBiz', action='store', dest='multiBiz')
        # https://yuque.alibaba-inc.com/lhubic/wuhf65/aecyg6
        self.parser.add_option('', '--kmonSinkAddress', action='store', dest='kmonSinkAddress',
                               default='127.0.0.1')
        self.parser.add_option('', '--specialCatalogList', action='store', dest='specialCatalogList')

    def parseParams(self, optionList):
        self.optionList = optionList
        self.addOptions()
        (options, args) = self.parser.parse_args(optionList)
        self.options = options
        ret = self.checkOptionsValidity(options)
        if not ret:
            print "ERROR: checkOptionsValidity Failed!"
            return False
        self.initMember(options)
        return True

    def checkOptionsValidity(self, options):
        if options.indexPath == None or options.indexPath == '':
            print "ERROR: index path must be specified"
            return False
        if options.configPath == None or options.configPath == '':
            print "ERROR: config path must be specified"
            return False
        if options.multiBiz == None or options.multiBiz == '':
            self.enableMultiBiz = False
        else:
            self.enableMultiBiz = True
        return True

    def initMember(self, options):
        self.workDir = os.getcwd()
        self.label = options.label
        self.partitionIndex = options.partitionIndex
        self.partitionCount = options.partitionCount
        self.role = options.role
        
        self.qrsQueue = options.qrsQueue
        self.searcherQueue = options.searcherQueue
        self.qrsThreadNum = options.qrsThreadNum
        self.qrsQueueSize = options.qrsQueueSize
        self.searcherThreadNum = options.searcherThreadNum
        self.searcherQueueSize = options.searcherQueueSize
        self.indexPath = options.indexPath
        self.aggName = options.aggName
        self.basicTuringBizNames = options.basicTuringBizNames
        self.basicTuringPrefix = options.basicTuringPrefix
        self.paraSearchWays = options.paraSearchWays
        self.disableSql = options.disableSql
        self.disableSqlWarmup = options.disableSqlWarmup
        self.dailyrunMode = options.dailyrunMode
        self.multiBiz = options.multiBiz
        self.kmonSinkAddress = options.kmonSinkAddress
        self.specialCatalogList = options.specialCatalogList
        if not self.indexPath.startswith('/'):
            self.indexPath = os.path.join(os.getcwd(), self.indexPath)
        self.configPath = options.configPath
        if not self.configPath.startswith('/'):
            self.configPath = os.path.join(os.getcwd(), self.configPath)
        self.onlineConfigPath = os.path.join(self.configPath, "bizs")
        configVersions = sorted(map(lambda x:int(x), os.listdir(self.onlineConfigPath)))
        if len(configVersions) == 0:
            print "config version count is 0, path [%s]"% self.onlineConfigPath
        else:
            self.onlineConfigPath = os.path.join(self.onlineConfigPath, str(configVersions[-1]))
        self.offlineConfigPath = os.path.join(self.configPath, "table")
        tableVersions = sorted(map(lambda x:int(x), os.listdir(self.offlineConfigPath)))
        if len(tableVersions) == 0:
            print "table version count is 0, path [%s]"% self.offlineConfigPath
        else:
            self.offlineConfigPath = os.path.join(self.offlineConfigPath, str(tableVersions[-1]))

        self.qrsHttpArpcBindPort = options.qrsHttpArpcBindPort
        self.qrsArpcBindPort = options.qrsArpcBindPort
        self.portList = []
        self.origin_port_list = map(lambda x:int(x), options.portList.split(","))
        self.searcher_port_list = []
        self.qrs_port_list = None

        self.portStart = 12000
        if len(self.portList) > 0:
            self.portStart = int(self.portList[0])
        else:
            self.portList = [12000]
        if options.zoneName:
            self.specialZones = options.zoneName.split(",")
        else:
            self.specialZones = []

        self.atableList = {}
        for zone in self.specialZones:
            self.atableList[zone] = ""
        if options.atables:
            azones = options.atables.split(";")
            for azone in azones:
                tmp = azone.split(":")
                self.atableList[tmp[0]] = tmp[1]
        if options.binaryPath:
            self.binaryPath = options.binaryPath
            if not options.binaryPath.startswith('/'):
                self.binaryPath = os.path.join(os.getcwd(), options.binaryPath)
        else:
            curdir = os.path.split(os.path.realpath(__file__))[0]
            self.binaryPath = os.path.join(curdir, "../../../../../../")
        self.timeout = options.timeout
        self.preload = options.preload
        self.serviceName = options.serviceName
        self.amonPort = options.amonPort
        self.libPath = "/opt/taobao/java/jre/lib/amd64/server/:/ha3_depends/home/admin/sap/lib64/:/ha3_depends/home/admin/diamond-client4cpp/lib:/ha3_depends/home/admin/eagleeye-core/lib/:/ha3_depends/usr/local/cuda-10.1/lib64/stubs:/ha3_depends/usr/local/cuda-10.1/lib64/:/ha3_depends/usr/local/lib64:/ha3_depends/usr/local/lib:/ha3_depends/usr/lib64/:/ha3_depends/usr/lib/:/usr/local/lib64:"  + self.binaryPath + "/usr/local/lib64:" + self.binaryPath + "/usr/lib:" + self.binaryPath + "/usr/lib64:" + self.binaryPath + "/usr/local/lib:"
        self.binPath = self.binaryPath + "/usr/local/bin/:" + self.binaryPath + "/usr/bin/:" + self.binaryPath + "/bin/:" + "/usr/local/bin:/usr/bin:/bin:/ha3_depends/home/admin/sap/bin/"
        self.startCmdTemplate = "/bin/env HIPPO_DP2_SLAVE_PORT=19715"
        if self.preload:
            if self.preload == "asan":
                self.startCmdTemplate += " LD_PRELOAD=/usr/lib/gcc/x86_64-redhat-linux/8/libasan.so  ASAN_OPTIONS=\"detect_leaks=1 handle_segv=0 disable_coredump=0 malloc_context_size=100\""
            else:
                self.startCmdTemplate += " LD_PRELOAD=%s" % self.preload

        self.startCmdTemplate += " JAVA_HOME=/usr/local/java HADOOP_HOME=/usr/local/hadoop/hadoop"
        if self.dailyrunMode:
            self.startCmdTemplate += " DAILY_RUN_MODE=true"
        self.startCmdTemplate += " PATH=$JAVA_HOME/bin:%s LD_LIBRARY_PATH=%s sap_server_d -l %s -r %s -c %s --port arpc:%d --port http:%d --env httpPort=%d --env serviceName=%s --env amonitorPath=%s/%s --env amonitorPort=%s --env roleType=%s --env port=%d --env ip=%s --env userName=admin --env decodeUri=true --env haCompatible=true --env zoneName=%s --env roleName=%s_partition_%d --env partId=0 --env decisionLoopInterval=10000 --env loadThreadNum=1"

        self.alogConfigPath = os.path.join(self.binaryPath, "usr/local/etc/ha3/ha3_alog.conf")
        self.searchCfg = os.path.join(self.binaryPath, "usr/local/etc/ha3/search_server.cfg")
        self.qrsCfg = os.path.join(self.binaryPath, "usr/local/etc/ha3/qrs_server.cfg")
        self.ip = socket.gethostbyname(socket.gethostname())
        self.readyZones = {}
        self.localSearchDir = os.path.join(os.getcwd(), "local_search_%d" % self.portStart)
        if not os.path.exists(self.localSearchDir):
            os.system("mkdir %s" % self.localSearchDir)
        binarySymbol = "%s/binary" % (self.localSearchDir)
        if not os.path.exists(binarySymbol):
            os.symlink(self.binaryPath, binarySymbol)
        self.pidFile = os.path.join(self.localSearchDir, "pid")
        self.portFile = os.path.join(self.localSearchDir, "ports")
        self.start_time = datetime.now()

    def run(self):
        tryTimes = 1
        status = None
        while tryTimes > 0:
            tryTimes -= 1
            ret, status = self.start_once()
            if ret == 1:
                # retry
                continue
            else:
                break
        return status

    def start_once(self):
        if not self.stopAll():
            return -1, ("", "stop exist pid failed", -1)
        
        if self.role == "searcher":
            searcher_ret = self.startSearcher()
            if searcher_ret == 0:
                return 0, ("", "", 0)
            else:
                return searcher_ret, ("", "start searcher failed", -1)
        else:
            qrs_ret = self.startQrs()
            if qrs_ret == 0:
                return 0, ("", "", 0)
            else:
                return qrs_ret, ("", "start qrs failed", -1)

    def stopAll(self, timeout = 120):
        needKillPids = []
        if os.path.exists(self.pidFile):
            f = open(self.pidFile)
            for line in f.readlines():
                line = line.strip('\n')
                parts = line.split(" ")
                if len(parts) >= 2:
                    pids = self.getPids(parts[1])
                    if int(parts[0]) in pids:
                        needKillPids.append((int(parts[0]), parts[1]))
        #print needKillPids
        if len(needKillPids) == 0:
            return True
        for (pid, port) in needKillPids:
            cmd = "kill -9 %s" % pid
            print "clean exist process, cmd [%s]" % cmd
            os.system(cmd)
        while timeout > 0:
            allKilled = True
            defunctPids = self.getDefunctPids()
            for (pid, rundir) in needKillPids:
                if pid in defunctPids:
                    allKilled = False
                pids = self.getPids(rundir)
                if len(pids) != 0:
                    allKilled = False
            if allKilled:
                cmd ="rm %s" % self.pidFile
                print "remove pid file [%s]" % self.pidFile
                os.system(cmd)
                break
            time.sleep(0.1)
            timeout -= 0.1
        return timeout > 0

    def startSearcher(self):
        zoneNames = self._getNeedStartZoneName()
        tableInfos = self._genTargetInfos(zoneNames)
        if not self._startSearcher(tableInfos):
            return -1
        print "wait searcher target load"
        target = self._loadSearcherTarget(tableInfos, self.timeout)
        self.writeReadyZones()
        return target

    def writeReadyZones(self):
        with open("{}/heartbeats/ready_zones.json".format(self.workDir), "w") as fp:
            json.dump(self.readyZones, fp)

    def startQrs(self):
        if not self._startQrs():
            return -1
        print "wait qrs target load"
        return self._loadQrsTarget(self.timeout)

    def curl(self, address, method, request, curl_type='POST'):
        try:
            conn = httplib.HTTPConnection(address, timeout=3)
            conn.request(curl_type, method, json.dumps(request))
            r = conn.getresponse()
            data = r.read()
            retCode = 0
            if r.status != 200:
                retCode = -1
            return retCode, data, r.reason, r.status
        except Exception, e:
            return -1, '', str(e), 418

    def createRuntimedirLink(self, zoneName, partId):
        zoneDir = os.path.join(self.localSearchDir, zoneName + "_" + str(partId))
        if not os.path.exists(zoneDir):
            os.makedirs(zoneDir)
        runtimeDir = os.path.join(zoneDir, "runtimedata")
        if not os.path.exists(runtimeDir):
            try:
                os.symlink(self.indexPath, runtimeDir)
            except Exception, e:
                print "create link failed, src %s, dst %s, e[%s]" % (self.indexPath, runtimeDir, str(e))
                raise e
        return runtimeDir

    def createConfigLink(self, zoneName, prefix, bizName, config):
        pos = config.rfind('/')
        version = config[pos+1:]
        rundir = os.path.join(self.localSearchDir, zoneName)
        bizConfigDir = os.path.join(rundir, "zone_config", prefix, bizName)
        if not os.path.exists(bizConfigDir):
            os.makedirs(bizConfigDir)
        fakeConfigPath = os.path.join(bizConfigDir, version)
        try:
            os.path.exists(fakeConfigPath) and shutil.rmtree(fakeConfigPath)
            cmd = 'cp -r "{}" "{}"'.format(config, fakeConfigPath)
            os.system(cmd)
        except Exception, e:
            print "copy config dir failed, src %s, dst %s, e[%s]" % (config, fakeConfigPath, str(e))
            raise e
        doneFile = os.path.join(fakeConfigPath, "suez_deploy.done")
        if not os.path.exists(doneFile):
            open(doneFile, 'a').close()
        return config

    def _loadQrsTarget(self, timeout = 300):
        readyZones = {}
        with open("{}/heartbeats/qrs_subscribe.json".format(self.workDir), "r") as f:
            readyZones = json.load(f)
        print("qrs readyZones {}".format(readyZones))
        max_version = max([int(zone["version"]) for zone in readyZones])
        for zone in readyZones:
            zone["version"] = max_version
        # coord_service = CoordinateClient("")
        # reponse = coord_service.read()
        # if self.label in reponse:
        #     readyZones = reponse[self.label]
        # else:
        #     readyZones = {}
        
        target = {
            "service_info" : {
                "cm2_config" : {
                    "local" : readyZones
                },
                "part_count" : 0,
                "part_id" : 0,
                "zone_name": "qrs"
            },
            "biz_info" : {
                "default" : {
                    "config_path" : self.createConfigLink('qrs', 'biz', 'default', self.onlineConfigPath)
                }
            },
            "table_info" : {
            },
            "clean_disk" : False
        }
        targetStr = ''
        if self.enableMultiBiz:
            target_multiBiz = {
                "service_info" : {
                    "cm2_config" : {
                        "local" : readyZones.values()
                    },
                    "part_count" : 0,
                    "part_id" : 0,
                    "zone_name": "qrs"
                },
                "biz_info" : {
                    "default" : {
                        "config_path" : self.createConfigLink('qrs', 'biz', 'default', self.onlineConfigPath)
                    },
                    self.multiBiz : {
                        "config_path" : self.createConfigLink('qrs', 'biz', self.multiBiz, self.onlineConfigPath)
                    }
                },
                "table_info" : {
                },
                "clean_disk" : False
            }
            targetStr = json.dumps(target_multiBiz)
        else:
            targetStr = json.dumps(target)
        requestSig = targetStr
        globalInfo = {"customInfo":targetStr}
        targetRequest = { "signature" : requestSig,
                          "customInfo" : targetStr,
                          "globalCustomInfo": json.dumps(globalInfo)
        }
        portList = self._getQrsPortList()
        httpArpcPort = portList[0]
        arpcPort = portList[1]
        address = "%s:%d" %(self.ip, httpArpcPort)
        while timeout > 0:
            time.sleep(0.1)
            timeout -= 0.1
            log_file = os.path.join(self.localSearchDir, "qrs", 'logs/ha3.log')
            log_state = self.check_log_file(log_file)
            if log_state != 0:
                return log_state

            retCode, out, err, _ = self.curl(address, "/HeartbeatService/heartbeat", targetRequest)
            if retCode != 0:
                print "set qrs target %s failed." % targetStr
                continue
            response = json.loads(out)
            if response["signature"] == requestSig:
                print "start local search success\nqrs is ready for search, http and arpc port: %s %s" % (httpArpcPort, arpcPort)
                return 0
        if timeout > 0:
            return 0
        else:
            return -1

    def check_log_file(self, log_file):
        if os.path.isfile(log_file):
            content = open(log_file, 'r').read()
            splited = filter(bool, content.split('\n'))
            if self.has_log('has been stopped', splited):
                return 1
            if self.has_log('initBiz failed', splited):
                return -1
        return 0

    def has_log(self, pattern, log_array):
        for line in reversed(log_array):
            if pattern in line:
                return self.valid_log(line)
        return False

    def valid_log(self, content):
        splited = filter(bool, content.split(']'))
        if len(splited) < 2:
            return False
        timestamp = splited[0]
        if timestamp[0] != '[':
            return False
        timestamp = timestamp[1:]
        log_time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
        return (log_time - self.start_time).total_seconds() > 0

    def _loadSearcherTarget(self, targetInfos, timeout = 300):
        self.readyZones = {}
        while timeout > 0:
            time.sleep(0.1)
            timeout -= 0.1
            count = 0
            for targetInfo in targetInfos:
                portList = self._getSearcherPortList(count)
                count += 1
                zoneName = targetInfo[0]
                partId = targetInfo[1]
                roleName = zoneName + "_" + str(partId)
                if self.readyZones.has_key(roleName) :
                    continue
                target = targetInfo[2]
                httpArpcPort = portList[0]
                arpcPort = portList[1]
                targetStr = json.dumps(target)
                requestSig = targetStr
                globalInfo = {"customInfo":targetStr}
                targetRequest = { "signature" : requestSig,
                                  "customInfo" :targetStr,
                                  "globalCustomInfo": json.dumps(globalInfo)
                }
                log_file = os.path.join(self.localSearchDir, roleName, 'logs/ha3.log')
                log_state = self.check_log_file(log_file)
                if log_state != 0:
                    return log_state
                address = "%s:%d" %(self.ip, httpArpcPort)
                retCode, out, err, _ = self.curl(address, "/HeartbeatService/heartbeat", targetRequest)
                if retCode != 0:
                    print "set target %s failed." % targetStr
                    continue
                response = json.loads(out)
                infos = []
                if response["signature"] == requestSig:
                    serviceInfo = json.loads(response["serviceInfo"])
                    infos = serviceInfo["cm2"]["topo_info"].strip('|').split('|')
                    for info in infos:
                        splitInfo = info.split(':')
                        localConfig = {}
                        localConfig["biz_name"] = splitInfo[0]
                        localConfig["part_count"] = int(splitInfo[1])
                        localConfig["part_id"] = int(splitInfo[2])
                        localConfig["version"] = int(splitInfo[3])
                        localConfig["ip"] = self.ip
                        localConfig["tcp_port"] = arpcPort
                        if splitInfo[0] == 'default':
                            self.readyZones[roleName] = localConfig
                            print "searcher [%s] is ready for search, topo [%s]" % (roleName, json.dumps(localConfig))
                        else:
                            zoneName = splitInfo[0] + "_" + str(partId)
                            self.readyZones[zoneName] = localConfig
                            print "searcher [%s] is ready for search, topo [%s]" % (zoneName, json.dumps(localConfig))
                elif timeout <= 0:
                    print "searcher [%s] load target [%s] failed" % (roleName, targetStr)
                if len(infos) > 0 and len(targetInfos) == (len(self.readyZones) / len(infos)):
                    print "all searcher is ready."
                    return 0
        if timeout > 0:
            return 0
        else:
            return -1

    def _startQrs(self):
        zoneName = "qrs"
        partId = 0
        rundir = os.path.join(self.localSearchDir, zoneName)
        if not os.path.exists(rundir):
            os.system("mkdir %s" % rundir)
        targetCfg = os.path.join(rundir, "qrs_service_%d.cfg" % (self.portStart))
        os.system("cp %s %s" % (self.qrsCfg, targetCfg))
        startCmd = self.startCmdTemplate % (self.binPath, self.libPath, self.alogConfigPath,
                                            self.binaryPath, targetCfg, 0, 0,
                                            self.qrsHttpArpcBindPort, self.serviceName,self.serviceName,
                                            zoneName, self.amonPort, "qrs", self.qrsArpcBindPort, self.ip, zoneName, zoneName, partId)
        if self.qrsQueue :
            startCmd += " --env extraTaskQueues=" + self.qrsQueue
        if self.qrsQueueSize :
            startCmd += " --env queueSize=" + str(self.qrsQueueSize)
        if self.qrsThreadNum :
            startCmd += " --env threadNum=" + str(self.qrsThreadNum)
        if self.aggName:
            startCmd += " --env defaultAgg=" + self.aggName
        if self.basicTuringBizNames:
            startCmd += " --env basicTuringBizNames=" + self.basicTuringBizNames
        if self.basicTuringPrefix:
            startCmd += " --env basicTuringPrefix=" + self.basicTuringPrefix
        if self.paraSearchWays:
            startCmd += " --env paraSearchWays=" + self.paraSearchWays
        if self.disableSql:
            startCmd += " --env disableSql=true"
        if self.disableSqlWarmup:
            startCmd += " --env disableSqlWarmup=true"
        if self.kmonSinkAddress:
            startCmd += " --env kmonitorSinkAddress=" + self.kmonSinkAddress;
        if self.specialCatalogList:
            startCmd += " --env specialCatalogList=" + str(self.specialCatalogList)

        startCmd += ' -d -n >> %s_qrs.asan  2>&1 ' % (self.pidFile)
        os.chdir(rundir)
        print "start qrs cmd: %s"% startCmd
        os.system(startCmd)
        time.sleep(0.3)
        pids = self.getPids(rundir)
        if len(pids) != 1:
            print "start qrs process failed, cmd [%s]"% cmd
            return False
        else:
            print "start qrs process success, pid [%d]" % pids[0]

        pid = pids[0]

        f = open(self.pidFile, 'a+')
        f.write("%d %s\n" % (pid, rundir))
        f.close()

        self.wait_load(rundir)
        status, http_port, arpc_port = self.get_listen_ports(pid)
        if not status:
            print 'get qrs listen port failed, pid [%d]' % pid
            return False
        self.qrs_port_list = (int(http_port), int(arpc_port))
        with open(self.portFile, 'w') as portFile:
            portFile.write('%s %s\n' % (http_port, arpc_port));
        return True

    def _startSearcher(self, targetInfos):
        count = 0
        f = open(self.pidFile, 'w')
        httpArpcPort_list = []
        for targetInfo in targetInfos:
            zoneName = targetInfo[0]
            partId = targetInfo[1]
            roleName = zoneName + "_%d" % partId
            rundir = os.path.join(self.localSearchDir, roleName)
            if not os.path.exists(rundir):
                os.system("mkdir %s" % rundir)
            targetCfg = os.path.join(rundir, zoneName + "_%d_search_service_%d.cfg" % (partId, self.portStart))
            os.system("cp %s %s" % (self.searchCfg, targetCfg))
            startCmd = self.startCmdTemplate % (self.binPath, self.libPath, self.alogConfigPath,
                                                self.binaryPath, targetCfg, 0, 0, 0, self.serviceName, self.serviceName,
                                                zoneName, self.amonPort, "searcher", 0, self.ip, zoneName, zoneName, partId )
            if self.searcherQueue :
                startCmd += " --env extraTaskQueues=" + self.searcherQueue
            if self.searcherQueueSize :
                startCmd += " --env queueSize=" + str(self.searcherQueueSize)
            if self.searcherThreadNum :
                startCmd += " --env threadNum=" + str(self.searcherThreadNum)
            if self.aggName:
                startCmd += " --env defaultAgg=" + self.aggName
            if self.paraSearchWays:
                startCmd += " --env paraSearchWays=" + self.paraSearchWays
            if self.basicTuringBizNames:
                startCmd += " --env basicTuringBizNames=" + self.basicTuringBizNames
            if self.kmonSinkAddress:
                startCmd += " --env kmonitorSinkAddress=" + self.kmonSinkAddress;

            startCmd += ' -d -n >> %s_searcher.asan  2>&1 ' % (self.pidFile)
            os.chdir(rundir)
            os.system(startCmd)
            httpArpcPort_list.append((rundir, roleName, startCmd))
            count = count + 1

        time.sleep(0.01)
        for info in httpArpcPort_list:
            rundir = info[0]
            roleName = info[1]
            startCmd = info[2]
            pids = self.getPids(rundir)
            print pids
            if len(pids) != 1:
                print "start searcher process [%s] failed, cmd [%s], rundir[%s]" % (roleName, startCmd, rundir)
                f.close()
                return False
            else:
                print "start searcher process [%s] success, pid [%d]" % (roleName, pids[0])

            pid = pids[0]
            f.write("%d %s\n" % (pid, rundir))
            self.wait_load(rundir)
            status, http_port, arpc_port = self.get_listen_ports(pid)
            if not status:
                print 'get searcher listen port failed, pid [%d]' % pid
                return False
            self.searcher_port_list.append((int(http_port), int(arpc_port)))

        f.close()
        return True

    def wait_load(self, rundir):
        while True:
            cmd = 'tail -1 %s/logs/ha3.log' % rundir
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            out, err = p.communicate()
            if 'HeartbeatManager.cpp -- getTarget' in out:
                if self.valid_log(out):
                    return True
            time.sleep(0.1)

    def get_listen_ports(self, pid):
        cmd = 'lsof -i -a -P -n -sTCP:LISTEN -p %s' % str(pid)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        ports = []
        for line in out.splitlines():
            if ':' in line:
                ports.append(line.split(':')[1].split(' ')[0])
        httpPort = 0
        arpcPort = 0
        for port in ports:
            address = "%s:%s" % (self.ip, port)
            _, data, _, status_code = self.curl(address, '/__method__', '', 'GET')
            if '/HeartbeatService/heartbeat' in data:
                httpPort = port
            elif status_code != 404:
                arpcPort = port
        ret = httpPort != 0 and arpcPort != 0
        print "%s %s" % (httpPort, arpcPort)
        return ret, httpPort, arpcPort

    def _getSearcherPortList(self, pos):
        return self.searcher_port_list[pos]

    def _getQrsPortList(self):
        return self.qrs_port_list

    def _getArpcPortList(self):
        arpcPortList = []
        for (httpArpcPort, arpcPort, httpPort) in self.portList:
            arpcPortList.append(arpcPort)
        return arpcPortList

    def _getHttpArpcPortList(self):
        httpArpcPortList = []
        for (httpArpcPort, arpcPort, httpPort) in self.portList:
            httpArpcPortList.append(httpArpcPort)
        return httpArpcPortList

    def _genTargetInfos(self, zoneNames):
        targetInfos = []
        for zoneName in zoneNames:
            tableInfos = {}
            for tableName in os.listdir(self.indexPath):
                curTableGid = self._getMaxGenerationId(self.indexPath, tableName)
                curTablePartition = self._getPartitions(self.indexPath, tableName, curTableGid)[0]

                # curTablePartitionCnt = len(curTablePartitions)
                # if curTablePartitionCnt == 1:
                #     tablePartition = fullPartition
                # elif curTablePartitionCnt == partitionsCnt:
                #     tablePartition = partition
                # else:
                #     raise Exception("table %s : len(curTablePartitions)(%d) != len(partitions)(%d)" % (tableName, curTablePartitionCnt, partitionsCnt))

                tableGid = self._getMaxGenerationId(self.indexPath, tableName)
                tableVersion = self._getMaxIndexVersion(self.indexPath, tableName, tableGid, curTablePartition)
                tmp = {
                tableGid : {
                    "index_root" : self.createRuntimedirLink(zoneName, self.partitionIndex),
                    "config_path": self.createConfigLink(zoneName + "_" + str(self.partitionIndex), 'table', tableName, self.offlineConfigPath),
                    "partitions": {
                    curTablePartition: {
                        "inc_version" : tableVersion
                    }
                    }
                }
                }
                tableInfos[tableName] = tmp
            targetInfo = {
                "table_info" : tableInfos,
                "service_info" : {
                    "part_count" : self.partitionCount,
                    "part_id" : self.partitionIndex,
                    "zone_name": zoneName,
                    "version" : curTableGid
                },
                "biz_info" : {
                    "default" : {
                        "config_path" : self.createConfigLink(zoneName + "_" + str(self.partitionIndex), 'biz', 'default', self.onlineConfigPath)
                    }
                },
                "clean_disk" : False
            }
            targetInfos.append((zoneName, self.partitionIndex, targetInfo))
                # partId = partId + 1
        return targetInfos
    
    def _getMaxGenerationId(self, indexPath, tableName):
        generationList = os.listdir(os.path.join(indexPath, tableName))
        versions = map(lambda x:int(x.split('_')[1]), generationList)
        versions.sort()
        return versions[len(versions) - 1]

    def _getMaxIndexVersion(self, path, clusterName, generationId, partition):
        files = os.listdir(os.path.join(path, clusterName, 'generation_' + str(generationId), 'partition_' + partition))
        versions = map(lambda x:int(x.split('.')[1]),
                       filter(lambda x:x.startswith('version.'), files))
        return sorted(versions)[-1]

    def _getPartitions(self, path, clusterName, generationId):
        partitions = os.listdir(os.path.join(path, clusterName, 'generation_' + str(generationId)))
        partitions = map(lambda x:x.split('n_')[1], filter(lambda x:len(x.split("_")) == 3, partitions))
        # sort by from of partitions
        partitions.sort(key=lambda x:int(x.split('_')[0]))
        # partitions = []
        # for part_interval in get_partition_intervals(self.partitionCount):
        #     part_name = str(part_interval[0]) + "_" + str(part_interval[1])
        #     partitions.append(part_name)
        return partitions

    def _getNeedStartZoneName(self):
        realPath = os.path.join(self.onlineConfigPath, 'zones')
        zones = os.listdir(realPath)
        if len(self.specialZones) == 0:
            return zones
        zoneNames = []
        for zone in zones:
            if zone in self.specialZones:
                zoneNames.append(zone)
        return zoneNames

    def getDefunctPids(self):
        cmd = 'ps uxww | grep sap_server_d | grep defunct| grep -v grep'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out,err = p.communicate()
        pids = []
        for line in out.splitlines():
            parts = line.split()
            pids.append(int(parts[1]))
        return pids

    def getPids(self, rundir):
        pids = []
        cmd = 'ps uxww | grep sap_server_d| grep "%s"| grep -v grep' % rundir
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out,err = p.communicate()
        for line in out.splitlines():
            parts = line.split()
            pids.append(int(parts[1]))
        return pids

    def getUnusedPort(self, lackPort = 1):
        sockets = []
        ports = []
        for i in range(lackPort):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('', 0))
            s.listen(1)
            addr, port = s.getsockname()
            ports.append(port)
            sockets.append(s)
        for s in sockets:
            s.close()
        return ports

if __name__ == '__main__':
    cmd = PartSearchStartCmd()
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
