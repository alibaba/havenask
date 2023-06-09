import fs_util_delegate
import httplib
import json
import time

class BuildId():
    def __init__(self, appName, dataTableName, generationId):
        self.buildId = {"buildId":{}}
        self.buildId["buildId"]["appName"] = appName
        self.buildId["buildId"]["dataTable"] = dataTableName
        self.buildId["buildId"]["generationId"] = generationId

    def getAppName(self):
        return self.buildId["buildId"]["appName"]
    
    def getDataTableName(self):
        return self.buildId["buildId"]["dataTable"]
    
    def getGenerationId(self):
        return self.buildId["buildId"]["generationId"]

    def toString(self):
        return json.dumps(self.buildId)

    def getBuildId(self):
        return self.buildId['buildId']
    
class BSTaskDuplicator():
    def __init__(self, originalBsAdminZk, targetBsAdminZk, madroxZk):
        self.madroxZk = madroxZk
        self.originalBsAdminZk = originalBsAdminZk
        self.targetBsAdminZk = targetBsAdminZk
        #legacy code for dev env fsutil copy index
        import tools_config
        self.toolsConf = tools_config.ToolsConfig()
        self.fsUtil = fs_util_delegate.FsUtilDelegate(self.toolsConf)

    def __getHTTPAddress(self, zkRoot, isBsAdminHttp = True):
        data = ""
        if isBsAdminHttp:
            data = self.fsUtil.cat(zkRoot + "/admin/LeaderElection")
        else:
            data = self.fsUtil.cat(zkRoot + "/master/LeaderElection")
            
        try:
            addressJson = json.loads(data.strip())
            return addressJson['httpAddress']
        except Exception, e:
            raise Exception("get HTTP address from zk failed, got data[%s]" % str(data))
        
    def __http_post(self, address, method, request):
        print ("Master HTTP Address: [%s]" % address)
        print ("METHOD: [%s]" % method)
        conn = httplib.HTTPConnection(address)
        conn.request("POST", method, json.dumps(request))
        rep = conn.getresponse()
        if rep.status != 200:
            raise Exception("http_post failed, address[%s], method[%s], request[%s]" % (address, method, str(request)))
        ret = json.loads(rep.read(), strict=False)
        conn.close()
        return ret

    def __getStatus(self, address, buildId):
        method = '/AdminService/getServiceInfo'
        request = {
            'appName' : buildId.getAppName(),
            'buildId' : buildId.getBuildId()
        }
        return self.__http_post(address, method, request)

    def __getGenerationInfo(self, gsStatus):
        if not (gsStatus.has_key('generationInfos')):
            raise Exception("gsStatus don't has generationInfos, gsStatus[%s] " % (str(gsStatus)))
        if (len(gsStatus['generationInfos']) != 1):
            raise Exception("gsStatus has two or more generationInfos, gsStatus[%s] " % (str(gsStatus)))
        return gsStatus['generationInfos'][0]

    def __getAllClusterInfos(self, gsStatus):
        generationInfo = self.__getGenerationInfo(gsStatus)
        if not (generationInfo.has_key('buildInfo')):
            raise Exception("generationInfo don't has buildInfo, generationInfo[%s] " % (str(generationInfo)))
        buildInfo = generationInfo['buildInfo']

        if not (buildInfo.has_key('clusterInfos')):
            raise Exception("buildInfo don't has clusterInfos, buildInfo[%s] " % (str(buildInfo)))
        if (len(buildInfo['clusterInfos']) == 0):
            raise Exception("buildInfo don't has clusterInfos, buildInfo[%s] " % (str(buildInfo)))
        gsClusterInfos = buildInfo['clusterInfos']

        clusterInfos = []
        for clusterInfo in gsClusterInfos:
            clusterName = ""
            partitionCount = 0
            if not (clusterInfo.has_key('clusterName')):
                raise Exception("clusterInfo don't has clusterName, clusterInfo[%s] " % (str(clusterInfo)))
            else:
                clusterName = clusterInfo['clusterName']

            if not (clusterInfo.has_key('partitionCount')):
                raise Exception("clusterInfo don't has partitionCount, clusterInfo[%s] " % (str(clusterInfo)))
            else:
                partitionCount = clusterInfo['partitionCount']                
            clusterInfos.append((clusterName, partitionCount))
        return clusterInfos
    
    def __getLastVersion(self, gsStatus, clusterName):
        generationInfo = self.__getGenerationInfo(gsStatus)
        if not (generationInfo.has_key('indexInfos')):
            raise Exception("generationInfo don't has indexInfos, generationInfo[%s] " % (str(generationInfo)))
        if (len(generationInfo['indexInfos']) == 0):
            raise Exception("generationInfo don't has indexInfos, generationInfo[%s] " % (str(generationInfo)))
        indexInfos = generationInfo['indexInfos']
        for indexInfo in indexInfos:
            if (indexInfo['clusterName'] == clusterName):
                return indexInfo['indexVersion']
        raise Exception("indexInfos don't has cluster[%s], indexInfos[%s] " % ((clusterName), (str(indexInfos))))

    def __getDataDescription(self, gsStatus):
        generationInfo = self.__getGenerationInfo(gsStatus)
        if not (generationInfo.has_key('dataDescriptions')):
            raise Exception("generationInfo don't has dataDescriptions, generationInfo[%s] " % (str(generationInfo)))
        return generationInfo['dataDescriptions']

    def __constructTargetDataDescription(self, dataDescription, swiftStartTimestampInSec):
        dataList = json.loads(dataDescription)
        if (len(dataList) == 0):
            raise Exception("dataDescription is empty. ")
        if (dataList[-1]['type'] != 'swift'):
            raise Exception("last dataDescription is not swift type, dataDescription[%s] " % (dataDescription))
        targetList = []
        targetList.append(dataList[-1])
        targetList[-1]['swift_start_timestamp'] = str(swiftStartTimestampInSec * 1000000)
        return json.dumps(targetList)

    def __alreadySnapshot(self, errorMsgs):
        alreadySnapshotMsg = 'version is already a snapshot version'
        for errorMsg in errorMsgs:
            if alreadySnapshotMsg in errorMsg:
                return True
        return False
    
    def __lockVersion(self, address, buildId, clusterName, snapshotVersion):
        method = '/AdminService/createSnapshot'
        request = {
            'buildId' : buildId.getBuildId(),
            'clusterName' : clusterName,
            'versionId' : snapshotVersion
        }
        ret = self.__http_post(address, method, request)
        if ret and ret.has_key("errorMessage") and not self.__alreadySnapshot(ret['errorMessage']):
            raise Exception("lock version [%d] failed:[%s]" % (snapshotVersion, str(ret)))
        
    def __unlockVersion(self, address, buildId, clusterName, snapshotVersion):
        method = '/AdminService/removeSnapshot'
        request = {
            'buildId' : buildId.getBuildId(),
            'clusterName' : clusterName,
            'versionId' : snapshotVersion
        }
        ret = self.__http_post(address, method, request)
        if ret and ret.has_key("errorMessage"):
            raise Exception("unlock version [%d] failed" % (snapshotVersion))

    def __getGenerationPath(self, indexRoot, buildId, clusterName):
        return indexRoot + "/" + clusterName + "/generation_" + str(buildId.getGenerationId()) + "/"
    
    def __getTargetIndexRoot(self, targetConfigPath):
        import build_app_config
        buildAppConf = build_app_config.BuildAppConfig(self.fsUtil, targetConfigPath)
        if (buildAppConf.indexRoot == None):
            raise Exception("build_app.json in targetConfigPath[%s] no index root" % (targetConfigPath))
        return buildAppConf.indexRoot

    def __copyIndex(self, originalGenerationPath, snapshotVersion, partitionCount, targetGenerationPath):
        if (self.madroxZk == None):
            #only for test
            self.fsUtil.copy(originalGenerationPath, targetGenerationPath)
        else:
            self.__copyIndexWithMadrox(originalGenerationPath, snapshotVersion, partitionCount, targetGenerationPath)
        return

    def __getRanges(self, partitionCount):
        ranges = []
        rangeFrom, rangeTo = (0, 65535)
        if partitionCount == 0 or rangeFrom > rangeTo:
            return []
        rangeCount = rangeTo - rangeFrom + 1
        c = rangeCount / partitionCount
        m = rangeCount % partitionCount
        begin = rangeFrom
        i = 0
        while i < partitionCount and begin <= rangeTo:
            a = (0 if i >= m else 1) - 1
            end = begin + c + a
            ranges.append((begin, end))
            begin = end + 1
            i += 1
        return ranges

    def __updateMadrox(self, address, request):
        method = '/MasterService/update'
        ret = self.__http_post(address, method, request)
        if ret and ret["errorInfo"]["errorCode"] == 'MASTER_ERROR_NONE':
            return
        else:
            raise Exception("update madrox failed, errorInfo [%s]" % (str(ret)))

    def __getMadroxStatus(self, address, targetGenerationPath):
        method = '/MasterService/getStatus'
        request = {}
        destRoot = [targetGenerationPath]
        request["destRoot"] = destRoot
        ret = self.__http_post(address, method, request)
        
        if ret and ret.has_key("status"):
            return ret["status"]
        else:
            raise Exception("cannot get madrox task status, errorInfo [%s]" % (str(ret)))
            
    def __copyIndexWithMadrox(self, originalGenerationPath, snapshotVersion, partitionCount, targetGenerationPath):
        ranges = self.__getRanges(partitionCount)
        request = {}
        deployMetas = []
        for partRange in ranges:
            partMeta = "partition_" + str(partRange[0]) + "_" + str(partRange[1]) + "/deploy_meta." + str(snapshotVersion)
            deployMetas.append(partMeta)

        request["deployMeta"] = deployMetas
        request["destRoot"] = [targetGenerationPath]
        request["srcRoot"] = originalGenerationPath
        request["removeOuter"] = False
        madroxHttp = self.__getHTTPAddress(self.madroxZk, False)
        self.__updateMadrox(madroxHttp, request)
        
        while True:
            madroxHttp = self.__getHTTPAddress(self.madroxZk, False)
            status = self.__getMadroxStatus(madroxHttp, targetGenerationPath)
            if status == 'DONE':
                print "copy index with madrox done"
                break
            elif status == 'RUNNING':
                print "Task [%s], waiting for copy index done" % (str(status))
            elif status == 'CANCELED':
                raise Exception("Task [%s] by madrox" % (str(status)))
            else:
                self.__updateMadrox(madroxHttp, request)
                print "Task [%s], retry update madrox task" % (str(status))
            time.sleep(10)
            
    def __startBuild(self, address, buildId, targetConfigPath, targetDataDescription):
        method = '/AdminService/startBuild'
        request = {
            'buildId' : buildId.getBuildId(),
            'dataDescriptionKvs' : targetDataDescription,
            'configPath' : targetConfigPath,
            'buildMode' : "incremental"
        }
        ret = self.__http_post(address, method, request)
        if ret.has_key("errorMessage") and len(ret["errorMessage"]) > 0:
            raise Exception("start build [%s] fail for [%s]" % (buildId.toString(), str(ret)))
            
    def duplicate(self, originalBuildId, targetBuildId, targetConfigPath, targetIndexRoot, swiftTopicStartTimestamp):
        originalHttpAddress = self.__getHTTPAddress(self.originalBsAdminZk)
        targetHttpAddress = self.__getHTTPAddress(self.targetBsAdminZk)
        originalGsStatus = self.__getStatus(originalHttpAddress, originalBuildId)
        dataDescription = self.__getDataDescription(originalGsStatus)
        generationInfo = self.__getGenerationInfo(originalGsStatus)
        indexRoot = generationInfo["indexRoot"]
        targetDataDescription = self.__constructTargetDataDescription(dataDescription, swiftTopicStartTimestamp)
        
        clusterInfos = self.__getAllClusterInfos(originalGsStatus)
        for clusterInfo in clusterInfos:
            clusterName = clusterInfo[0]
            partitionCount = clusterInfo[1]
            snapshotVersion = self.__getLastVersion(originalGsStatus, clusterName)

            if (snapshotVersion == None):
                raise Exception("duplicate build task [%s] failed : no index version to duplicate" % (originalBuildId.toString()))

            self.__lockVersion(originalHttpAddress, originalBuildId, clusterName, snapshotVersion)
            originalGenerationPath = self.__getGenerationPath(indexRoot, originalBuildId, clusterName)
            targetGenerationPath = self.__getGenerationPath(targetIndexRoot, targetBuildId, clusterName)
            self.__copyIndex(originalGenerationPath, snapshotVersion, partitionCount, targetGenerationPath)
            self.__unlockVersion(originalHttpAddress, originalBuildId, clusterName, snapshotVersion)
            
        self.__startBuild(targetHttpAddress, targetBuildId, targetConfigPath, targetDataDescription)
        
