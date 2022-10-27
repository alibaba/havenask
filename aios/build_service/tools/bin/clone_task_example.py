import sys
import os

currentDir = os.path.dirname(os.path.realpath(__file__))
dependPath=currentDir + "/../etc/bs_tools/"
sys.path.append(dependPath)

dependPath = currentDir + "/../lib/python/site-packages/bs_tools/"
sys.path.append(dependPath)

import build_task_duplicator

def getBuildId(buildIdStr):
    buildId = buildIdStr.split(";")
    if len(buildId) != 3:
        print "illegal build id [%s]"%(buildIdStr)
        exit()
        
    if not buildId[2].isdigit():
        print "illegal generation id [%s]"%(buildIdStr)
        exit()
        
    appName = buildId[0]
    dataTable = buildId[1]
    generationId = int(buildId[2])
    return appName,dataTable,generationId 


if __name__ == '__main__':
    originalZkRoot = raw_input("input original task info [bs zkRoot]:")
    originalBuildId = raw_input("input original buildId [appName;dataTable;generationId]:")
    originalAppName,originalDataTable,originalGenerationId = getBuildId(originalBuildId)

    targetZkRoot = raw_input("input target task info [bs zkRoot]:")
    targetBuildId = raw_input("input target buildId [appName;dataTable;generationId]:")
    targetAppName,targetDataTable,targetGenerationId = getBuildId(targetBuildId)

    targetIndexRoot = raw_input("input target task info [targetIndexRoot]:")
    targetConfigPath = raw_input("input target task info [configPath]:")
    swiftStartTimestampInSec = raw_input("input target task info [swiftStartTimestamp in second]:")
    madroxZkRoot = raw_input("input madroxZkRoot[optional, empty use fs_util]:")

    if not swiftStartTimestampInSec.isdigit():
        print "invalid swift start timestamp [%s]"%(swiftStartTimestamp)
        exit()
    swiftStartTimestampInSec = int(swiftStartTimestampInSec)

    if (not madroxZkRoot):
        madroxZkRoot = None
    duplicator = build_task_duplicator.BSTaskDuplicator(originalZkRoot, targetZkRoot, madroxZkRoot)
    
    originalBuildId = build_task_duplicator.BuildId(originalAppName, originalDataTable, originalGenerationId)
    targetBuildId = build_task_duplicator.BuildId(targetAppName, targetDataTable, targetGenerationId)
    
    duplicator.duplicate(originalBuildId, targetBuildId, targetConfigPath, targetIndexRoot, swiftStartTimestampInSec)
