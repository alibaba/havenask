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
import errno
from datetime import datetime
from itertools import product


class TimeoutTerminator:
    def __init__(self, timeout):
        self.timeout = timeout
        self.start = time.time()
        self.due = self.start + timeout

    def left_time(self):
        return self.due - time.time()

    def is_timeout(self):
        return self.left_time() <= 0

    def raw_timeout(self):
        return self.timeout

    def start_str(self):
        return datetime.utcfromtimestamp(self.start).strftime('%Y-%m-%d %H:%M:%S.%f')


class PortListItem:
    def __init__(self):
        self.ports = None
        self.role = None


class LocalSearchStartCmd(object):
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
    {-M model_biz           | --modelBiz=model_biz}
    {-L local_biz_service   | --localBizService=localBizService}
options:
    -i index_dir,     --index=index_dir              : required, index dir
    -c config_dir,    --config=config_dir            : required, config path,
    -p port_list,     --port=port_list               : optional, port list, http port is first port, arpc port is second port, default 12000, if only one port is designed, next port is start +1 (total port may use start + n*3 )
    -z zone_name,     --zone=zone_name               : optional, special zone to start
    -j auxiliary_table, --tables=auxiliary_table     : optional, special auxiliary table to load
    -b binary_path,   --binary=binary_path           : optional, special binary path to load
    -t timeout,       --timeout=timeout              : optional, special timeout load [defalut 300]
    -l preload,       --preload=preload              : optional, special lib to load
    -s serviceName,   --serviceName=serviceName      : optional, serviceName [default sql_suez_local_search]
    -a amonPath,      --amonPath=amonPath            : optional, amon path [default sql_suez_local_search]
    -g agg_name,      --aggName=aggName              : optional, aggName [default default_agg_4]
    --basicTuringBizNames=names                      : optional, common turing biz names, example [biz1,biz2]
    --tabletInfos=tabletInfos                        : optional, tabletInfos [default empty str]
    --basicTuringPrefix=prefix                       : optional, common turing request http prefix, example [/common]
    -w para_search_ways, --paraSearchWays=paraSearchWays  : optional, paraSearchWays [default 2,4]
    -S disable_sql,   --disableSql=true              : optional, disableSql [default false]
    -W disable_sql_warmup, --disableSqlWarmup=true   : optional, disableSqlWarmup [default false]
    -d dailyrun mode, --dailyrunMode=true            : optional, enable dailyrun mode
    -m multi_biz,        --multiBiz=multiBiz         : optional, enable multi_biz
    -M model_biz,        --modelBiz=modelBiz         : optional, modelBiz list
    -L local_biz_service, --localBizService=localBizService : optional, [defalut false]
    --enableMultiPartition                           : optional, enableMultiPartition [default false]
    --enableLocalAccess                              : optional, enableLocalAccess [default false]
    --onlySql                                        : optional, onlySql [default false]
    --enableLocalCatalog                             : optional, enableLocalCatalog [default false]
    --enableUpdateCatalog                            : optional, enableUpdateCatalog [default false]
    --zk_root                                        : optional, zk_root for leader election [default LOCAL]
    --mode                                           : optional, mode for table direct write
    --leader_election_strategy_type                  : optional, leader election type
    --leader_election_config                         : optional, leader election config
    --version_sync_config                            : optional, version sync config
    --disableCodeGen                                 : optional, disable indexlib codegen
    --disableTurbojet                                : optional, disable turbojet
    --searcherLocalSubscribe                         : optional, searcher will subscribe each other
    --enablePublishTableTopoInfo                     : optional, searcher will publish topo info by table
    --force_tablet_load                              : optional, searcher will load with tablet

examples:
    ./local_search_starter.py -i /path/to/index -c path/to/config
    ./local_search_starter.py -i /path/to/index -c path/to/config -p 12345
    ./local_search_starter.py -i /path/to/index -c path/to/config -p 12345,22345,32345
    ./local_search_starter.py -i /path/to/index -c path/to/config -z zone1,zone2 -j table1:t1,t3;table2:t3
    '''

    def __init__(self):
        self.parser = OptionParser(usage=self.__doc__)

    def usage(self):
        print self.__doc__ % {'prog': sys.argv[0]}

    def addOptions(self):
        self.parser.add_option('-i', '--index', action='store', dest='indexPath')
        self.parser.add_option('-c', '--config', action='store', dest='configPath')
        self.parser.add_option('-p', '--port', action='store', dest='portList')
        self.parser.add_option('', '--qrsHttpArpcBindPort', dest='qrsHttpArpcBindPort', type='int', default=0)
        self.parser.add_option('', '--qrsArpcBindPort', dest='qrsArpcBindPort', type='int', default=0)

        self.parser.add_option('-z', '--zone', action='store', dest='zoneName')
        self.parser.add_option('-j', '--tables', action='store', dest='atables')
        self.parser.add_option('-b', '--binary', action='store', dest='binaryPath')
        self.parser.add_option('-t', '--timeout', action='store', dest='timeout', type='int', default=300)
        self.parser.add_option('-l', '--preload', action='store', dest='preload', default='asan')
        self.parser.add_option(
            '-s',
            '--serviceName',
            action='store',
            dest='serviceName',
            default="sql_suez_local_search")
        self.parser.add_option('-a', '--amonPath', action='store', dest='amonPath', default="sql_suez_local_search")
        self.parser.add_option('-g', '--aggName', action='store', dest='aggName')
        self.parser.add_option('', '--basicTuringBizNames', action='store', dest='basicTuringBizNames')
        self.parser.add_option('', '--tabletInfos', action='store', dest='tabletInfos')
        self.parser.add_option('', '--basicTuringPrefix', action='store', dest='basicTuringPrefix')
        self.parser.add_option('-w', '--paraSearchWays', action='store', dest='paraSearchWays')
        self.parser.add_option('-S', '--disableSql', action='store_true', dest='disableSql', default=False)
        self.parser.add_option('-W', '--disableSqlWarmup', action='store_true', dest='disableSqlWarmup', default=False)
        self.parser.add_option('', '--qrsExtraQueue', action='store', dest='qrsQueue')
        self.parser.add_option('', '--searcherExtraQueue', action='store', dest='searcherQueue')
        self.parser.add_option('', '--searcherThreadNum', action='store', dest='searcherThreadNum', type='int')
        self.parser.add_option('', '--searcherQueueSize', action='store', dest='searcherQueueSize', type='int')
        self.parser.add_option('', '--naviThreadNum', action='store', dest='naviThreadNum', type='int')
        self.parser.add_option('', '--threadNumScaleFactor', action='store', dest='threadNumScaleFactor', type='float')
        self.parser.add_option('', '--qrsThreadNum', action='store', dest='qrsThreadNum', type='int')
        self.parser.add_option('', '--qrsQueueSize', action='store', dest='qrsQueueSize', type='int')
        self.parser.add_option('-d', '--dailyrunMode', action='store_true', dest='dailyrunMode', default=False)
        self.parser.add_option(
            '',
            '--enableMultiPartition',
            action='store_true',
            dest='enableMultiPartition',
            default=False)
        self.parser.add_option('', '--enableLocalAccess', action='store_true', dest='enableLocalAccess', default=False)
        self.parser.add_option('', '--onlySql', action='store_true', dest='onlySql', default=False)
        self.parser.add_option(
            '',
            '--enableLocalCatalog',
            action='store_true',
            dest='enableLocalCatalog',
            default=False)
        self.parser.add_option(
            '',
            '--enableUpdateCatalog',
            action='store_true',
            dest='enableUpdateCatalog',
            default=False)
        self.parser.add_option('-m', '--multiBiz', action='store_true', dest='multiBiz')
        self.parser.add_option('-M', '--modelBiz', action='store', dest='modelBiz', default='')
        self.parser.add_option('-L', '--localBizService', action='store_true', dest='localBizService')
        # xxxx://invalid/lhubic/wuhf65/aecyg6
        self.parser.add_option('', '--kmonSinkAddress', action='store', dest='kmonSinkAddress',
                               default='127.0.0.1')
        self.parser.add_option('', '--specialCatalogList', action='store', dest='specialCatalogList')
        self.parser.add_option('', '--zk_root', action='store', dest='zkRoot', default='LOCAL')
        self.parser.add_option('', '--mode', action='store', dest='mode', default='rw')
        self.parser.add_option('', '--leader_election_strategy_type', action='store', dest='leaderElectionStrategyType')
        self.parser.add_option('', '--leader_election_config', action='store', dest='leaderElectionConfig')
        self.parser.add_option('', '--version_sync_config', action='store', dest='versionSyncConfig')
        self.parser.add_option('', '--searcherReplica', type=int, dest='searcherReplica', default=1)
        self.parser.add_option('', '--disableCodeGen', action='store_true', dest='disableCodeGen', default=False)
        self.parser.add_option('', '--disableTurbojet', action='store_true', dest='disableTurbojet', default=False)
        self.parser.add_option('', '--searcherSubscribeConfig', action='store', dest='searcherSubscribeConfig', )
        self.parser.add_option(
            '',
            '--searcherLocalSubscribe',
            action='store_true',
            dest='searcherLocalSubscribe',
            default=False)
        self.parser.add_option(
            '',
            '--enablePublishTableTopoInfo',
            action='store_true',
            dest='enablePublishTableTopoInfo',
            default=False)
        self.parser.add_option('', '--force_tablet_load', action='store_true', dest='forceTabletLoad', default=False)
        self.parser.add_option('', '--allow_follow_write', action='store_true', dest='allowFollowWrite', default=False)

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
        if options.indexPath is None or options.indexPath == '':
            print "ERROR: index path must be specified"
            return False
        if options.configPath is None or options.configPath == '':
            print "ERROR: config path must be specified"
            return False
        if options.multiBiz is None or options.multiBiz == '':
            self.enableMultiBiz = False
        else:
            self.enableMultiBiz = True
        return True

    def initMember(self, options):
        self.searcherReplica = options.searcherReplica
        self.qrsQueue = options.qrsQueue
        self.searcherQueue = options.searcherQueue
        self.qrsThreadNum = options.qrsThreadNum
        self.qrsQueueSize = options.qrsQueueSize
        self.searcherThreadNum = options.searcherThreadNum
        self.naviThreadNum = options.naviThreadNum
        self.searcherQueueSize = options.searcherQueueSize
        self.threadNumScaleFactor = options.threadNumScaleFactor
        self.indexPath = options.indexPath
        self.aggName = options.aggName
        self.basicTuringBizNames = options.basicTuringBizNames
        self.tabletInfos = options.tabletInfos
        self.basicTuringPrefix = options.basicTuringPrefix
        self.paraSearchWays = options.paraSearchWays
        self.disableSql = options.disableSql
        self.disableSqlWarmup = options.disableSqlWarmup
        self.enableMultiPartition = options.enableMultiPartition
        self.enableLocalAccess = options.enableLocalAccess
        self.onlySql = options.onlySql
        self.enableLocalCatalog = options.enableLocalCatalog
        self.enableUpdateCatalog = options.enableUpdateCatalog
        self.dailyrunMode = options.dailyrunMode
        self.multiBiz = options.multiBiz
        self.modelBiz = set(options.modelBiz.split(','))
        self.localBizService = options.localBizService
        self.kmonSinkAddress = options.kmonSinkAddress
        self.specialCatalogList = options.specialCatalogList
        self.zkRoot = options.zkRoot
        self.mode = options.mode
        self.leaderElectionStrategyType = options.leaderElectionStrategyType
        self.leaderElectionConfig = options.leaderElectionConfig
        self.versionSyncConfig = options.versionSyncConfig
        self.disableCodeGen = options.disableCodeGen
        self.disableTurbojet = options.disableTurbojet
        self.searcherLocalSubscribe = options.searcherLocalSubscribe
        self.enablePublishTableTopoInfo = options.enablePublishTableTopoInfo
        self.forceTabletLoad = options.forceTabletLoad
        self.allowFollowWrite = options.allowFollowWrite
        if not self.indexPath.startswith('/'):
            self.indexPath = os.path.join(os.getcwd(), self.indexPath)
        self.configPath = options.configPath
        if not self.configPath.startswith('/'):
            self.configPath = os.path.join(os.getcwd(), self.configPath)
        self.onlineConfigPath = os.path.join(self.configPath, "bizs")
        self.offlineConfigPath = os.path.join(self.configPath, "table")
        tableVersions = sorted(map(lambda x: int(x), os.listdir(self.offlineConfigPath)))
        if len(tableVersions) == 0:
            print "table version count is 0, path [%s]" % self.offlineConfigPath
        else:
            self.offlineConfigPath = os.path.join(self.offlineConfigPath, str(tableVersions[-1]))

        self.qrsHttpArpcBindPort = options.qrsHttpArpcBindPort
        self.qrsArpcBindPort = options.qrsArpcBindPort
        self.portList = []
        self.origin_port_list = map(lambda x: int(x), options.portList.split(","))
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
        self.binaryPath = os.path.abspath(self.binaryPath)
        self.timeout = options.timeout
        self.preload = options.preload
        self.serviceName = options.serviceName
        self.amonPath = options.amonPath
        self.libPath = "/usr/ali/java/jre/lib/amd64/server:/usr/local/java/jdk/jre/lib/amd64/server:" + self.binaryPath + "/home/admin/sap/lib64/:" + self.binaryPath + "/home/admin/diamond-client4cpp/lib:" + self.binaryPath + "/home/admin/eagleeye-core/lib/:" + self.binaryPath + "/usr/local/cuda-10.1/lib64/stubs:" + \
            self.binaryPath + "/usr/local/cuda-10.1/lib64/:" + self.binaryPath + "/usr/local/lib64:" + self.binaryPath + "/usr/lib:" + self.binaryPath + "/usr/lib64:" + self.binaryPath + "/usr/local/lib:" + "/home/admin/isearch5_data/AliWS-1.4.0.0/usr/local/lib:" + "/usr/local/lib64:"
        self.binPath = self.binaryPath + "/home/admin/sap/bin/:" + self.binaryPath + "/usr/local/bin/:" + \
            self.binaryPath + "/usr/bin/:" + self.binaryPath + "/bin/:" + "/usr/local/bin:/usr/bin:/bin"
        self.startCmdTemplate = "/bin/env HIPPO_DP2_SLAVE_PORT=19715"
        if self.preload:
            if self.preload == "asan":
                self.startCmdTemplate += " LSAN_OPTIONS=\"suppressions=%s/usr/local/etc/sql/leak_suppression\" ASAN_OPTIONS=\"detect_odr_violation=0 abort_on_error=1 detect_leaks=1\"" % (
                    self.binaryPath)
            elif self.preload == "llalloc":
                self.startCmdTemplate += " LD_PRELOAD=%s/usr/local/lib64/libllalloc.so" % self.binaryPath
            else:
                self.startCmdTemplate += " LD_PRELOAD=%s" % self.preload

        self.startCmdTemplate += " JAVA_HOME=/usr/local/java HADOOP_HOME=/usr/local/hadoop/hadoop CLASSPATH=/usr/local/hadoop/hadoop/etc/hadoop:/usr/local/hadoop/hadoop/share/hadoop/common/lib/*:/usr/local/hadoop/hadoop/share/hadoop/common/*:/usr/local/hadoop/hadoop/share/hadoop/hdfs:/usr/local/hadoop/hadoop/share/hadoop/hdfs/lib/*:/usr/local/hadoop/hadoop/share/hadoop/hdfs/*:/usr/local/hadoop/hadoop/share/hadoop/yarn/lib/*:/usr/local/hadoop/hadoop/share/hadoop/yarn/*:/usr/local/hadoop/hadoop/share/hadoop/mapreduce/lib/*:/usr/local/hadoop/hadoop/share/hadoop/mapreduce/*:/usr/local/hadoop/hadoop/contrib/capacity-scheduler/*.jar "
        if self.dailyrunMode:
            self.startCmdTemplate += " DAILY_RUN_MODE=true"
            self.startCmdTemplate += " IS_TEST_MODE=true"
        if self.disableCodeGen:
            self.startCmdTemplate += " DISABLE_CODEGEN=true"
        if self.disableTurbojet:
            self.startCmdTemplate += " DISABLE_TURBOJET=true"
        if self.enablePublishTableTopoInfo:
            self.startCmdTemplate += " ENABLE_PUBLISH_TABLE_TOPO_INFO=true"

        self.startCmdTemplate += " HIPPO_APP_INST_ROOT=" + self.binaryPath + \
            " HIPPO_APP_WORKDIR=" + os.getcwd() + " TJ_RUNTIME_TEMP_DIR=" + self.binaryPath
        self.startCmdTemplate += " PATH=$JAVA_HOME/bin:%s LD_LIBRARY_PATH=%s  ha_sql --env roleType=%s -l %s -r %s -c %s --port arpc:%d --port http:%d --env httpPort=%d --env gigGrpcPort=0 --env serviceName=%s --env amonitorPath=%s/%s --env port=%d --env ip=%s --env userName=admin --env decodeUri=true --env haCompatible=true --env zoneName=%s --env roleName=%s_partition_%d --env partId=0 --env decisionLoopInterval=10000 --env dpThreadNum=1 --env loadThreadNum=4 --env load_biz_thread_num=4 --env kmonitorNormalSamplePeriod=1 --env naviPoolModeAsan=1 --env naviDisablePerf=1 --env WORKER_IDENTIFIER_FOR_CARBON= --env gigGrpcThreadNum=5 --env GRPC_CLIENT_CHANNEL_BACKUP_POLL_INTERVAL_MS=500 --env FAST_CLEAN_INC_VERSION=false --env navi_config_loader=%s"
        if self.localBizService:
            self.startCmdTemplate += " --env localBizService=true"
        self.alogConfigPath = os.path.join(self.binaryPath, "usr/local/etc/sql/sql_alog.conf")
        self.searchCfg = os.path.join(self.binaryPath, "usr/local/etc/sql/search_server.cfg")
        self.qrsCfg = os.path.join(self.binaryPath, "usr/local/etc/sql/qrs_server.cfg")
        self.config_loader = os.path.join(self.binaryPath,
                                          "usr/local/lib/python/site-packages/sql/sql_config_loader.py")
        self.ip = socket.gethostbyname(socket.gethostname())
        self.gigInfos = {}
        cwd = os.getcwd()
        self.localSearchDir = os.path.join(cwd, "local_search_%d" % self.portStart)
        if not os.path.exists(self.localSearchDir):
            os.system("mkdir %s" % self.localSearchDir)
        binarySymbol = "%s/binary" % (self.localSearchDir)
        if not os.path.exists(binarySymbol):
            try:
                os.symlink(self.binaryPath, binarySymbol)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    os.remove(binarySymbol)
                    os.symlink(self.binaryPath, binarySymbol)
                else:
                    raise e

        self.pidFile = os.path.join(self.localSearchDir, "pid")
        self.portFile = os.path.join(self.localSearchDir, "ports")
        self.qrsPortFile = os.path.join(cwd, 'qrs_port')
        self.searcherPortFile = os.path.join(cwd, 'searcher_port')
        self.start_time = datetime.now()
        self.targetVersion = 1651870394

    def run(self):
        tryTimes = 1
        ret = -1
        status = None
        while tryTimes > 0:
            tryTimes -= 1
            ret, status = self.start_once()
            if ret == 1:
                # retry
                continue
            else:
                break
        if ret == 0:
            if self.qrs_port_list:
                with open(self.qrsPortFile, 'w') as f:
                    f.write(json.dumps(self.__format_port_list(self.qrs_port_list), indent=4))
            if len(self.searcher_port_list) > 0:
                port_map = [(x.role, self.__format_port_list(x.ports)) for x in self.searcher_port_list]
                with open(self.searcherPortFile, 'w') as f:
                    f.write(json.dumps(port_map, indent=4))
        return status

    def __format_port_list(self, port_list):
        return {
            'http_arpc_port': port_list[0],
            'arpc_port': port_list[1],
            'grpc_port': port_list[2]
        }

    def start_once(self):
        terminator = TimeoutTerminator(self.timeout)
        searcherTargetInfos = None
        if not self.stopAll():
            return -1, ("", "stop exist pid failed", -1)
        if not self.enableLocalAccess:
            searcher_ret, searcherTargetInfos = self.startSearcher()
            if searcher_ret != 0:
                return searcher_ret, ("", "start searcher failed", -1)
        else:
            zoneNames = self._getNeedStartZoneName()
            if len(zoneNames) > 1:
                return -1, ("", "local access mode only support one zone, now zone names " + str(zoneNames), -1)

        qrs_ret = self.startQrs()
        if qrs_ret != 0:
            return qrs_ret, ("", "start qrs failed", -1)

        if not self.enableLocalAccess:
            ret = self.loadSearcherTarget(searcherTargetInfos, terminator.left_time())
            if ret != 0:
                return ret, ("", "load searcher target failed", -1)

        if self.searcherLocalSubscribe:
            ret = self.subscribeLocalSearchService()
            if ret != 0:
                return ret, ("", "add local subscribe failed", -1)

        ret = self.loadQrsTarget(terminator.left_time())
        if ret != 0:
            return ret, ("", "load qrs target failed", -1)

        return 0, ("", "", 0)

    def getLocalSubscribeConfig(self, zoneName, grpcPort):
        bizName = zoneName + ".default_sql"
        local_config = {
            "part_count": 1,
            "biz_name": bizName,
            "version": 123,
            "part_id": 0,
            "ip": socket.gethostbyname(socket.gethostname()),
            "support_heartbeat": True,
            "grpc_port": int(grpcPort)
        }
        return local_config

    def subscribeLocalSearchService(self):
        targetInfos = self._genTargetInfos(self._getNeedStartZoneName(), self.searcherReplica)
        subscribeConfig = {
            "local": []
        }
        idx = 0
        for needSubscribeInfo in targetInfos:
            grpcPort = self._getSearcherPortList(idx)[2]
            idx += 1
            zoneName = needSubscribeInfo[0]
            local_config = self.getLocalSubscribeConfig(zoneName, grpcPort)
            subscribeConfig["local"].append(local_config)

        for targetInfo in targetInfos:
            target = targetInfo[3]
            target['service_info']['cm2_config'] = subscribeConfig
        terminator = TimeoutTerminator(self.timeout)
        ret = self.loadSearcherTarget(targetInfos, terminator.left_time())
        return ret

    def stopAll(self, timeout=120):
        terminator = TimeoutTerminator(timeout)
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
        # print needKillPids
        if len(needKillPids) == 0:
            return True
        for (pid, port) in needKillPids:
            cmd = "kill -9 %s" % pid
            print "clean exist process, cmd [%s]" % cmd
            os.system(cmd)
        while True:
            if terminator.is_timeout():
                return False
            allKilled = True
            defunctPids = self.getDefunctPids()
            for (pid, rundir) in needKillPids:
                if pid in defunctPids:
                    allKilled = False
                pids = self.getPids(rundir)
                if len(pids) != 0:
                    allKilled = False
            if allKilled:
                cmd = "rm %s" % self.pidFile
                print "remove pid file [%s]" % self.pidFile
                os.system(cmd)
                break
            time.sleep(0.1)
        return True

    def startSearcher(self):
        zoneNames = self._getNeedStartZoneName()
        targetInfos = self._genTargetInfos(zoneNames, replica=self.searcherReplica)
        if not self._startSearcher(targetInfos):
            print "start searcher with table info failed: ", json.dumps(targetInfos)
            return -1, None
        print "wait searcher target load"
        return 0, targetInfos

    def startQrs(self):
        if not self._startQrs():
            return -1
        print "wait qrs target load"
        return 0

    def curl(self, address, method, request, curl_type='POST', timeout=20):
        terminator = TimeoutTerminator(timeout)
        while True:
            try:
                timeout = terminator.left_time()
                conn = httplib.HTTPConnection(address, timeout=timeout)
                conn.request(curl_type, method, json.dumps(request))
                conn.sock.settimeout(10 if timeout > 10 else timeout)
                r = conn.getresponse()
                data = r.read()
                retCode = 0
                if r.status != 200:
                    retCode = -1
                return retCode, data, r.reason, r.status
            except Exception as e:
                if terminator.is_timeout():
                    return -1, '', str(e), 418

    def createRuntimedirLink(self, zoneDirName):
        zoneDir = os.path.join(self.localSearchDir, zoneDirName)
        if not os.path.exists(zoneDir):
            os.makedirs(zoneDir)
        runtimeDir = os.path.join(zoneDir, "runtimedata")
        if not os.path.exists(runtimeDir):
            try:
                os.symlink(self.indexPath, runtimeDir)
            except Exception as e:
                print "create link failed, src %s, dst %s, e[%s]" % (self.indexPath, runtimeDir, str(e))
                raise e
        return runtimeDir

    def genOnlineConfigPath(self, config, biz):
        onlineConfig = os.path.join(self.onlineConfigPath, biz)
        configVersions = sorted(map(lambda x: int(x), os.listdir(onlineConfig)))
        if len(configVersions) == 0:
            print "config version count is 0, path [%s]" % onlineConfig
        else:
            onlineConfig = os.path.join(onlineConfig, str(configVersions[-1]))
        return onlineConfig

    def createConfigLink(self, zoneName, prefix, bizName, config):
        pos = config.rfind('/')
        version = config[pos + 1:]
        rundir = os.path.join(self.localSearchDir, zoneName)
        bizConfigDir = os.path.join(rundir, "zone_config", prefix, bizName)
        if not os.path.exists(bizConfigDir):
            os.makedirs(bizConfigDir)
        fakeConfigPath = os.path.join(bizConfigDir, version)
        try:
            os.path.exists(fakeConfigPath) and shutil.rmtree(fakeConfigPath)
            cmd = 'cp -r "{}" "{}"'.format(config, fakeConfigPath)
            os.system(cmd)
        except Exception as e:
            print "copy config dir failed, src %s, dst %s, e[%s]" % (config, fakeConfigPath, str(e))
            raise e
        doneFile = os.path.join(fakeConfigPath, "suez_deploy.done")
        if not os.path.exists(doneFile):
            open(doneFile, 'a').close()
        return config

    def loadQrsTarget(self, timeout=300):
        terminator = TimeoutTerminator(timeout)
        bizs = os.listdir(self.onlineConfigPath)
        bizInfo = {}
        if self.enableMultiBiz:
            for biz in bizs:
                onlineConfig = self.genOnlineConfigPath(self.onlineConfigPath, biz)
                bizInfo[biz] = {
                    "config_path": self.createConfigLink('qrs', 'biz', biz, onlineConfig)
                }
                if biz in self.modelBiz:
                    bizInfo[biz]["custom_biz_info"] = {
                        "biz_type": "model_biz"
                    }
        else:
            onlineConfig = self.genOnlineConfigPath(self.onlineConfigPath, bizs[0])
            bizInfo['default'] = {
                "config_path": self.createConfigLink('qrs', 'biz', 'default', onlineConfig)
            }
        tableInfos = {}
        zoneName = "qrs"
        portList = self.getQrsPortList()
        httpArpcPort = portList[0]
        arpcPort = portList[1]
        grpcPort = portList[2]
        address = "%s:%d" % (self.ip, httpArpcPort)
        arpcAddress = "%s:%d" % (self.ip, arpcPort)

        if self.enableLocalAccess:
            zoneNames = self._getNeedStartZoneName()
            targetInfos = self._genTargetInfos(zoneNames, 1, True)
            tableInfos = targetInfos[0][3]["table_info"]
            zoneName = zoneNames[0]
        local_sub_config = self.gigInfos.values()
        if self.enableLocalAccess:
            local_sub_config = [self.getLocalSubscribeConfig("qrs", grpcPort)]

        # add default subscribe config for external table case
        gigInfos = self.gigInfos
        externalTableConfig = {}
        externalTableConfig["biz_name"] = "default.external"
        externalTableConfig["part_count"] = 1
        externalTableConfig["part_id"] = 0
        externalTableConfig["version"] = 0
        externalTableConfig["ip"] = self.ip
        externalTableConfig["tcp_port"] = arpcPort
        if grpcPort != 0:
            externalTableConfig["grpc_port"] = grpcPort
            externalTableConfig["support_heartbeat"] = False
        local_sub_config.append(externalTableConfig)

        target = {
            "service_info": {
                "cm2_config": {
                    "local": local_sub_config
                },
                "part_count": 1,
                "part_id": 0,
                "zone_name": zoneName
            },
            "biz_info": bizInfo,
            "table_info": tableInfos,
            "clean_disk": False,
            "catalog_address": arpcAddress,
            "target_version": self.targetVersion
        }
        targetStr = ''
        targetStr = json.dumps(target)
        requestSig = targetStr
        globalInfo = {"customInfo": targetStr}
        targetRequest = {"signature": requestSig,
                         "customInfo": targetStr,
                         "globalCustomInfo": json.dumps(globalInfo)
                         }
        lastRespSignature = ""
        while True:
            timeout = terminator.left_time()
            if timeout <= 0:
                break
            log_file = os.path.join(self.localSearchDir, "qrs", 'logs/sql.log')
            log_state = self.check_log_file(log_file)
            if log_state != 0:
                return log_state

            retCode, out, err, status = self.curl(
                address, "/HeartbeatService/heartbeat", targetRequest, timeout=timeout)
            if retCode != 0:  # qrs core
                print "set qrs target [{}] failed, address[{}] ret[{}] out[{}] err[{}] status[{}] left[{}]s".format(targetStr, address, retCode, out, err, status, terminator.left_time())
                return -1
            response = json.loads(out)
            if "signature" not in response:
                print "set qrs target response invalid [{}], continue...".format(out)
                continue
            lastRespSig = response["signature"]
            if lastRespSig == requestSig:
                print "start local search success\nqrs is ready for search, http arpc grpc port: %s %s %s" % (httpArpcPort, arpcPort, grpcPort)
                if self.enableLocalAccess:
                    time.sleep(1)
                return 0
            time.sleep(0.1)
        print 'load qrs target timeout [{}]s left[{}]s resp[{}] request[{}]'.format(terminator.raw_timeout(), terminator.left_time(), lastRespSig, requestSig)
        return -1

    def check_log_file(self, log_file):
        if os.path.isfile(log_file):
            expectLog = '\'has been stopped\''
            cmd = 'grep %s %s' % (expectLog, log_file)
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            out, err = p.communicate()
            splited = filter(bool, out.split('\n'))
            if self.has_log(expectLog, splited):
                return 1

            expectLog = '\'initBiz failed\''
            cmd = 'grep %s %s' % (expectLog, log_file)
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            out, err = p.communicate()
            splited = filter(bool, out.split('\n'))
            if self.has_log(expectLog, splited):
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

    def loadSearcherTarget(self, targetInfos, timeout=300):
        self.gigInfos = {}
        terminator = TimeoutTerminator(timeout)
        readyTarget = set()
        while True:
            timeout = terminator.left_time()
            if timeout <= 0:
                break
            count = 0
            for targetInfo in targetInfos:
                portList = self._getSearcherPortList(count)
                count += 1
                zoneName = targetInfo[0]
                partId = targetInfo[1]
                replicaId = targetInfo[2]
                roleName = self.genRoleName(targetInfo)
                if roleName in readyTarget:
                    continue

                target = targetInfo[3]
                if self.options.searcherSubscribeConfig:
                    target['service_info']['cm2_config'] = json.loads(self.options.searcherSubscribeConfig)
                httpArpcPort = portList[0]
                arpcPort = portList[1]
                grpcPort = portList[2]
                qrsArpcPort = self.getQrsPortList()[1]
                arpcAddress = "%s:%d" % (self.ip, qrsArpcPort)
                target["catalog_address"] = arpcAddress
                target["target_version"] = self.targetVersion
                targetStr = json.dumps(target)
                requestSig = targetStr
                globalInfo = {"customInfo": targetStr}
                targetRequest = {"signature": requestSig,
                                 "customInfo": targetStr,
                                 "globalCustomInfo": json.dumps(globalInfo)
                                 }
                log_file = os.path.join(self.localSearchDir, roleName, 'logs/sql.log')
                log_state = self.check_log_file(log_file)
                if log_state != 0:
                    return log_state
                address = "%s:%d" % (self.ip, httpArpcPort)
                retCode, out, err, status = self.curl(address, "/HeartbeatService/heartbeat",
                                                      targetRequest, timeout=timeout)

                if retCode != 0:  # binary core
                    print "set searcher target [{}] failed. role[{}] address[{}] ret[{}] out[{}] err[{}] status[{}] left[{}]s".format(
                        targetRequest, roleName, address, retCode, out, err, status, terminator.left_time())
                    return -1
                response = json.loads(out)
                infos = []
                if "signature" not in response:
                    print "set searcher target response invalid [{}] role [{}], continue...".format(out, roleName)
                    continue
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
                        if grpcPort != 0:
                            localConfig["grpc_port"] = grpcPort
                            localConfig["support_heartbeat"] = True
                        gigKey = roleName + "_" + splitInfo[0] + "_" + str(splitInfo[2])
                        self.gigInfos[gigKey] = localConfig
                    readyTarget.add(roleName)
                    print "searcher [%s] is ready for search, topo [%s]" % (roleName, json.dumps(localConfig))
                if len(targetInfos) == len(readyTarget):
                    print "all searcher is ready."
                    return 0
            time.sleep(0.1)
        print 'load searcher [{}] target [{}] timeout [{}]s left [{}]s readyTarget[{}]'.format(
            zoneName,
            targetStr,
            terminator.raw_timeout(),
            terminator.left_time(),
            readyTarget)
        return -1

    def _startQrs(self):
        zoneName = "qrs"
        partId = 0
        rundir = os.path.join(self.localSearchDir, zoneName)
        if not os.path.exists(rundir):
            os.system("mkdir %s" % rundir)
        targetCfg = os.path.join(rundir, "qrs_service_%d.cfg" % (self.portStart))
        os.system("cp %s %s" % (self.qrsCfg, targetCfg))
        startCmd = self.startCmdTemplate % (self.binPath, self.libPath, "qrs", self.alogConfigPath,
                                            self.binaryPath, targetCfg, 0, 0,
                                            self.qrsHttpArpcBindPort, self.serviceName, self.amonPath,
                                            zoneName, self.qrsArpcBindPort, self.ip, zoneName, zoneName,
                                            partId, self.config_loader)
        startCmd += " --env LIBHDFS_OPTS=-Xrs "
        startCmd += " --env JAVA_TOOL_OPTIONS=-Djdk.lang.processReaperUseDefaultStackSize=true "
        if self.qrsQueue:
            startCmd += " --env extraTaskQueues=" + self.qrsQueue
        if self.qrsQueueSize:
            startCmd += " --env queueSize=" + str(self.qrsQueueSize)
        if self.qrsThreadNum:
            startCmd += " --env threadNum=" + str(self.qrsThreadNum)
        if self.naviThreadNum:
            startCmd += " --env naviThreadNum=" + str(self.naviThreadNum)
        if self.threadNumScaleFactor:
            startCmd += " --env threadNumScaleFactor=" + str(self.threadNumScaleFactor)
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
            startCmd += " --env kmonitorSinkAddress=" + self.kmonSinkAddress
        if self.specialCatalogList:
            startCmd += " --env specialCatalogList=" + str(self.specialCatalogList)
        if self.forceTabletLoad:
            startCmd += " --env force_tablet_load=true"
        if self.enableLocalAccess:
            startCmd += " --env enableLocalAccess=true"
            startCmd += " --env rewriteLocalBizNameType=sql"
            if self.enableMultiPartition:
                startCmd += " --env enableMultiPartition=true"
            if self.enableUpdateCatalog and not self.disableSql:
                startCmd += " --env enableUpdateCatalog=true"
        if self.enableLocalCatalog and not self.disableSql:
            startCmd += " --env enableLocalCatalog=true"
        if self.onlySql:
            startCmd += " --env onlySql=true"
        if (self.tabletInfos):
            if self.mode:
                startCmd += " --env mode=" + self.mode
            if self.zkRoot:
                startCmd += " --env zk_root=" + self.zkRoot
            if self.leaderElectionStrategyType:
                startCmd += " --env leader_election_strategy_type=" + self.leaderElectionStrategyType
            if self.leaderElectionConfig:
                startCmd += " --env leader_election_config=" + "'" + self.leaderElectionConfig + "'"
            if self.versionSyncConfig:
                startCmd += " --env version_sync_config=" + "'" + self.versionSyncConfig + "'"

        startCmd += ' -d -n 1>>%s 2>>%s ' % (os.path.join(self.localSearchDir, "qrs.stdout.out"),
                                             os.path.join(self.localSearchDir, "qrs.stderr.out"))
        os.chdir(rundir)
        print "start qrs cmd: %s" % startCmd
        os.system(startCmd)

        time.sleep(0.1)
        terminator = TimeoutTerminator(5)
        while not terminator.is_timeout():
            pids = self.getPids(rundir)
            if len(pids) == 1:
                break
            time.sleep(0.1)

        if len(pids) != 1:
            print "start qrs process failed, cmd [%s]" % startCmd
            return False
        else:
            print "start qrs process success, pid [%d]" % pids[0]

        pid = pids[0]

        f = open(self.pidFile, 'a+')
        f.write("%d %s\n" % (pid, rundir))
        f.close()

        self.wait_load(rundir)
        http_port, arpc_port, grpc_port = self.get_listen_ports(rundir)
        self.qrs_port_list = (http_port, arpc_port, grpc_port)
        with open(self.portFile, 'w') as portFile:
            portFile.write('%s %s\n' % (http_port, arpc_port))
        return True

    def _startSearcher(self, targetInfos):
        count = 0
        f = open(self.pidFile, 'w')
        httpArpcPort_list = []
        for targetInfo in targetInfos:
            zoneName = targetInfo[0]
            partId = targetInfo[1]
            replicaId = targetInfo[2]
            roleName = self.genRoleName(targetInfo)
            rundir = os.path.join(self.localSearchDir, roleName)
            if not os.path.exists(rundir):
                os.system("mkdir %s" % rundir)
            targetCfg = os.path.join(rundir, zoneName + "_%d_search_service_%d.cfg" % (partId, self.portStart))
            cmd = "cp %s %s" % (self.searchCfg, targetCfg)
            print cmd
            os.system("cp %s %s" % (self.searchCfg, targetCfg))
            kmonServiceName = self.serviceName
            if '^' in self.serviceName:
                # override tags['zone'], tags['role'], tags['host']
                kmonServiceName = self.serviceName + '@zone^{}@role^{}@host^{}'.format(zoneName, partId, roleName)
            startCmd = self.startCmdTemplate % (self.binPath, self.libPath, "searcher", self.alogConfigPath,
                                                self.binaryPath, targetCfg, 0, 0, 0, kmonServiceName, self.amonPath,
                                                zoneName, 0, self.ip, zoneName, zoneName, partId, self.config_loader)
            if self.searcherQueue:
                startCmd += " --env extraTaskQueues=" + self.searcherQueue
            if self.searcherQueueSize:
                startCmd += " --env queueSize=" + str(self.searcherQueueSize)
            if self.threadNumScaleFactor:
                startCmd += " --env threadNumScaleFactor=" + str(self.threadNumScaleFactor)
            if self.searcherThreadNum:
                startCmd += " --env threadNum=" + str(self.searcherThreadNum)
            if self.naviThreadNum:
                startCmd += " --env naviThreadNum=" + str(self.naviThreadNum)
            if self.aggName:
                startCmd += " --env defaultAgg=" + self.aggName
            if self.paraSearchWays:
                startCmd += " --env paraSearchWays=" + self.paraSearchWays
            if self.basicTuringBizNames:
                startCmd += " --env basicTuringBizNames=" + self.basicTuringBizNames
            if self.kmonSinkAddress:
                startCmd += " --env kmonitorSinkAddress=" + self.kmonSinkAddress
            if self.enableMultiPartition:
                startCmd += " --env enableMultiPartition=true"
            if self.forceTabletLoad:
                startCmd += " --env force_tablet_load=true"
            if self.allowFollowWrite:
                startCmd += " --env ALLOW_FOLLOWER_WRITE=true"
            if self.enableLocalAccess:
                startCmd += " --env enableLocalAccess=true"
            else:
                if self.enableUpdateCatalog and not self.disableSql:
                    startCmd += " --env enableUpdateCatalog=true"
            if self.disableSql:
                startCmd += " --env disableSql=true"
            if self.onlySql:
                startCmd += " --env onlySql=true"
            if (self.tabletInfos):
                if self.mode:
                    startCmd += " --env mode=" + self.mode
                if self.zkRoot:
                    startCmd += " --env zk_root=" + self.zkRoot
                if self.leaderElectionStrategyType:
                    startCmd += " --env leader_election_strategy_type=" + self.leaderElectionStrategyType
                if self.leaderElectionConfig:
                    startCmd += " --env leader_election_config=" + "'" + self.leaderElectionConfig + "'"
                if self.versionSyncConfig:
                    startCmd += " --env version_sync_config=" + "'" + self.versionSyncConfig + "'"

            startCmd += ' -d -n 1>>%s 2>>%s ' % (os.path.join(self.localSearchDir,
                                                              "{}.stdout.out".format(roleName)),
                                                 os.path.join(self.localSearchDir,
                                                              "{}.stderr.out".format(roleName)))
            os.chdir(rundir)
            print "start searcher cmd: %s" % startCmd
            os.system(startCmd)
            httpArpcPort_list.append((rundir, roleName, startCmd))
            count = count + 1
        time.sleep(0.1)
        terminator = TimeoutTerminator(10)
        while not terminator.is_timeout():
            start = True
            for info in httpArpcPort_list:
                rundir = info[0]
                pids = self.getPids(rundir)
                if len(pids) != 1:
                    start = False
            if start:
                break
            time.sleep(0.1)
        if terminator.is_timeout():
            print "start searcher [%s] timeout, cmd [%s], rundir[%s]" % (roleName, startCmd, rundir)
        for info in httpArpcPort_list:
            rundir = info[0]
            roleName = info[1]
            startCmd = info[2]
            pids = self.getPids(rundir)
            print pids
            if len(pids) != 1:
                print "start searcher process [%s] failed, pids [%s] len(pid) != 1, cmd [%s], rundir[%s]" % (roleName, ','.join(pids), startCmd, rundir)
                f.close()
                return False
            else:
                print "start searcher process [%s] success, pid [%d]" % (roleName, pids[0])

            pid = pids[0]
            f.write("%d %s\n" % (pid, rundir))
            if not self.wait_load(rundir):
                print "wait load failed [{}]".format(rundir)
                return False
            http_port, arpc_port, grpc_port = self.get_listen_ports(rundir)
            item = PortListItem()
            item.ports = (http_port, arpc_port, grpc_port)
            item.role = roleName
            self.searcher_port_list.append(item)
        f.close()
        return True

    def wait_load(self, rundir, timeout=60):
        terminator = TimeoutTerminator(timeout)
        while True:
            timeout = terminator.left_time()
            if timeout <= 0:
                break
            time.sleep(0.1)
            heartbeatTarget = 'HeartbeatManager.cpp -- getTarget'
            cmd = 'grep "{}" {}/logs/sql.log | tail -1'.format(heartbeatTarget, rundir)
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            out, err = p.communicate()
            out = out.strip()
            if out:
                if self.valid_log(out):
                    sapTarget = '\"initWatchThread sucess\"'   # wait sap server ready
                    cmd = 'grep %s %s/logs/sql.log | tail -1' % (sapTarget, rundir)
                    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
                    out, err = p.communicate()
                    if self.valid_log(out):
                        return True
        print "wait load timeout [{}]s left[{}]s start[{}] for [{}]".format(terminator.raw_timeout(),
                                                                            terminator.left_time(),
                                                                            terminator.start_str(),
                                                                            rundir)
        return False

    def get_listen_ports(self, rundir):
        return self.get_listen_http_arpc_ports(rundir), self.get_listen_arpc_ports(
            rundir), self.get_listen_grpc_ports(rundir)

    def get_listen_arpc_ports(self, rundir):
        grpcListenKeyword = 'gigRpcServer initArpcServer success, arpc listen port'
        cmd = 'grep "{}" {}/logs/sql.log | tail -1'.format(grpcListenKeyword, rundir)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        out = out.strip()
        if out:
            if self.valid_log(out):
                return int(out.split('listen port [')[1].split(']')[0])
        return 0

    def get_listen_http_arpc_ports(self, rundir):
        grpcListenKeyword = 'gigRpcServer initHttpArpcServer success, httpArpc listen port'
        cmd = 'grep "{}" {}/logs/sql.log | tail -1'.format(grpcListenKeyword, rundir)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        out = out.strip()
        if out:
            if self.valid_log(out):
                return int(out.split('listen port [')[1].split(']')[0])
        return 0

    def get_listen_grpc_ports(self, rundir):
        grpcListenKeyword = 'gigRpcServer initGrpcServer success, grpc listen port'
        cmd = 'grep "{}" {}/logs/sql.log | tail -1'.format(grpcListenKeyword, rundir)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        out = out.strip()
        if out:
            if self.valid_log(out):
                return int(out.split('listen port [')[1].split(']')[0])
        return 0

    def _getSearcherPortList(self, pos):
        return self.searcher_port_list[pos].ports

    def getQrsPortList(self):
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

    def _genCatalogTable(self, dbName, tableGroupName, tableName, tableMode, tableVersion):
        return {
            "id": {
                "dbName": dbName,
                "tgName": tableGroupName,
                "tableName": tableName
            },
            "meta": {
                "mode": tableMode
            },
            "partitions": [{
                "id": {
                    "dbName": dbName,
                    "tgName": tableGroupName,
                    "tableName": tableName,
                    "partitionName": tableVersion
                },
                "meta": {
                    "fullVersion": tableVersion
                }
            }]
        }
    # tabletInfos = {tableName:{generationId:gid, partitions:["0_65535"], tableMode:1}}

    def _genTargetInfos(self, zoneNames, replica, inQrs=False):
        tabletInfos = {}
        if self.tabletInfos:
            tabletInfos = json.loads(self.tabletInfos)
        targetInfos = []
        for zoneName in zoneNames:
            tableGroup = {
                "id": {
                    "tgName": zoneName,
                    "dbName": zoneName
                },
                "meta": {
                    "shardCount": 1,
                    "replicaCount": 0
                },
                "tables": []
            }
            tableGroupTables = {}
            atables = []
            if self.atableList.has_key(zoneName):
                atables = self.atableList[zoneName].split(",")
            atables.append(zoneName)
            zoneGid = None
            partitions = None
            has_offline_index = True
            has_realtime = False
            if tabletInfos.has_key(zoneName):
                has_offline_index = tabletInfos[zoneName]['has_offline_index']
                has_realtime = True
            if has_offline_index:
                zoneGid = self._getMaxGenerationId(self.indexPath, zoneName)
                partitions = self._getPartitions(self.indexPath, zoneName, zoneGid)
            else:
                zoneGid = tabletInfos[zoneName]["generationId"]
                partitions = tabletInfos[zoneName]["partitions"]
            fullPartition = "0_65535"
            partCnt = len(partitions)
            if self.enableMultiPartition:
                partCnt = 1
            maxPartCnt = len(partitions)
            tableGroup["meta"]["shard_count"] = partCnt

            for replicaId, partId in product(range(replica), range(partCnt)):
                tableInfos = {}
                for tableName in atables:
                    if not tableName:
                        continue
                    tablePartition = []
                    curTableGid = None
                    curTablePartitions = None

                    has_offline_index = True
                    has_realtime = False
                    if tabletInfos.has_key(tableName):
                        has_offline_index = tabletInfos[tableName]['has_offline_index']
                        has_realtime = True
                    if has_offline_index:
                        curTableGid = self._getMaxGenerationId(self.indexPath, tableName)
                        curTablePartitions = self._getPartitions(self.indexPath, tableName, curTableGid)
                    else:
                        curTableGid = tabletInfos[tableName]["generationId"]
                        curTablePartitions = tabletInfos[tableName]["partitions"]
                    if not self.enableMultiPartition:
                        curTablePartitionCnt = len(curTablePartitions)
                        if curTablePartitionCnt == 1:
                            tablePartition.append(fullPartition)
                        elif curTablePartitionCnt == maxPartCnt:
                            tablePartition.append(partitions[partId])
                        else:
                            raise Exception(
                                "table %s : len(curTablePartitions)(%d) != maxPartCnt(%d)" %
                                (tableName, curTablePartitionCnt, maxPartCnt))
                    else:
                        tablePartition = curTablePartitions

                    tableGid = curTableGid
                    zoneDirName = self.genRoleName((zoneName, partId, replicaId))
                    if inQrs:
                        zoneDirName = "qrs"
                    tableMode = 0 if not has_realtime else 1  # TM_READ_WRITE
                    tableType = 3 if not has_realtime else 2  # SPT_TABLET
                    tmp = {
                        tableGid: {
                            "table_mode": tableMode,
                            "table_type": tableType,
                            "index_root": self.createRuntimedirLink(zoneDirName),
                            "config_path": self.createConfigLink(
                                zoneDirName,
                                'table',
                                tableName,
                                self.offlineConfigPath),
                            "total_partition_count": len(curTablePartitions),
                            "partitions": {}}}
                    for partition in tablePartition:
                        incVersion = -1
                        if has_offline_index:
                            incVersion = self._getMaxIndexVersion(self.indexPath, tableName, tableGid, partition)
                        tmp[tableGid]["partitions"][partition] = {
                            "inc_version": incVersion
                        }
                    tableInfos[tableName] = tmp
                    tableGroupTables[tableName] = self._genCatalogTable(
                        zoneName, zoneName, tableName, tableMode, tableGid)
                bizs = os.listdir(self.onlineConfigPath)
                bizInfo = {}
                if self.enableMultiBiz:
                    for biz in bizs:
                        onlineConfig = self.genOnlineConfigPath(self.onlineConfigPath, biz)
                        bizInfo[biz] = {
                            "config_path": self.createConfigLink(zoneDirName, 'biz', biz, onlineConfig)
                        }
                        if biz in self.modelBiz:
                            bizInfo[biz]["custom_biz_info"] = {
                                "biz_type": "model_biz"
                            }
                else:
                    onlineConfig = self.genOnlineConfigPath(self.onlineConfigPath, bizs[0])
                    bizInfo['default'] = {
                        "config_path": self.createConfigLink(zoneDirName, 'biz', 'default', onlineConfig)
                    }
                targetInfo = {
                    "table_info": tableInfos,
                    "service_info": {
                        "part_count": partCnt,
                        "part_id": partId,
                        "zone_name": zoneName,
                        "version": zoneGid
                    },
                    "biz_info": bizInfo,
                    "clean_disk": False
                }
                targetInfos.append((zoneName, partId, replicaId, targetInfo))
            database = {
                "dbName": zoneName,
                "description": "",
                "tableGroups": [],
                "udxfs": {},
                "tvfs": {}
            }
            for tableName, catalogTable in tableGroupTables.items():
                tableGroup["tables"].append(catalogTable)

            database["tableGroups"].append(tableGroup)
        return targetInfos

    def _getMaxGenerationId(self, indexPath, tableName):
        generationList = os.listdir(os.path.join(indexPath, tableName))
        versions = map(lambda x: int(x.split('_')[1]), generationList)
        versions.sort()
        return versions[len(versions) - 1]

    def _getMaxIndexVersion(self, path, clusterName, generationId, partition):
        files = os.listdir(os.path.join(path, clusterName, 'generation_' + str(generationId), 'partition_' + partition))
        versions = map(lambda x: int(x.split('.')[1]),
                       filter(lambda x: x.startswith('version.'), files))
        if len(versions) > 0:
            return sorted(versions)[-1]
        return -1

    def _getPartitions(self, path, clusterName, generationId):
        partitions = os.listdir(os.path.join(path, clusterName, 'generation_' + str(generationId)))
        partitions = map(lambda x: x.split('n_')[1], filter(lambda x: len(x.split("_")) == 3, partitions))
        # sort by from of partitions
        partitions.sort(key=lambda x: int(x.split('_')[0]))
        return partitions

    def _getSchema(self, path, clusterName, generationId, partition):
        filePath = os.path.join(
            path,
            clusterName,
            'generation_' +
            str(generationId),
            'partition_' +
            partition,
            "schema.json")
        with open(filePath, "r") as f:
            schema = json.load(f)
        return schema

    def _getTableType(self, path, clusterName, generationId, partition):
        schema = self._getSchema(path, clusterName, generationId, partition)
        return schema["table_type"]

    def _getNeedStartZoneName(self):
        bizs = os.listdir(self.onlineConfigPath)
        zones = []
        for biz in bizs:
            onlineConfig = self.genOnlineConfigPath(self.onlineConfigPath, biz)
            realPath = os.path.join(onlineConfig, 'zones')
            zones += os.listdir(realPath)
        zones = list(set(zones))
        if len(self.specialZones) == 0:
            return zones
        zoneNames = []
        for zone in zones:
            if zone in self.specialZones:
                zoneNames.append(zone)
        return zoneNames

    def getDefunctPids(self):
        cmd = 'ps uxww | grep ha_sql | grep defunct| grep -v grep'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        pids = []
        for line in out.splitlines():
            parts = line.split()
            pids.append(int(parts[1]))
        return pids

    def getPids(self, rundir):
        pids = []
        cmd = 'ps uxww | grep ha_sql| grep "%s"| grep -v grep' % rundir
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        for line in out.splitlines():
            parts = line.split()
            pids.append(int(parts[1]))
        return pids

    def getUnusedPort(self, lackPort=1):
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

    def genRoleName(self, targetInfo):
        zoneName = targetInfo[0]
        partId = targetInfo[1]
        replicaId = targetInfo[2]
        return '{zoneName}_p{partId}_r{replicaId}'.format(zoneName=zoneName, partId=partId, replicaId=replicaId)


if __name__ == '__main__':
    cmd = LocalSearchStartCmd()
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
