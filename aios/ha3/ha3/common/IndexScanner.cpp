#include <ha3/common/IndexScanner.h>
#include <ha3/common/HaPathDefs.h>
#include <suez/turing/common/FileUtil.h>
#include <fslib/fslib.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace suez::turing;

USE_HA3_NAMESPACE(proto);

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, IndexScanner);

const string IndexScanner::INDEX_INVALID_FLAG = "_ha3_reserved_index_invalid_flag_";

IndexScanner::IndexScanner() { 
}

IndexScanner::~IndexScanner() { 
}

bool IndexScanner::scanIndex(const string &indexPath, IndexInfo &indexInfo, bool isOffline)
{
    indexInfo.clear();
    FileList fileList;
    ErrorCode errorCode = FileSystem::listDir(indexPath, fileList);
    if (EC_OK != errorCode) {
        HA3_LOG(ERROR, "Failed to list index dir:[%s], errorMsg:[%s]",
                indexPath.c_str(), FileSystem::getErrorString(errorCode).c_str());
        return false;
    }

    bool ret = true;
    for(FileList::const_iterator it = fileList.begin(); it != fileList.end(); ++it) {
        string clusterPath = FileUtil::joinFilePath(indexPath, (*it));
        assert((*it) != "." && (*it) != "..");
        errorCode = FileSystem::isDirectory(clusterPath);
        if (EC_TRUE == errorCode) {
            ClusterIndexInfo clusterIndexInfo;
            if (!scanCluster(clusterPath, clusterIndexInfo, isOffline)) {
                ret = false;
            } else {
                indexInfo[*it] = clusterIndexInfo;
            }
        } else if (EC_FALSE != errorCode) {
            HA3_LOG(ERROR, "Failed to judge [%s] isDirectory. errorMsg:[%s]",
                    clusterPath.c_str(), FileSystem::getErrorString(errorCode).c_str());
            ret = false;         
        }
    }
    return ret;
}

bool IndexScanner::scanClusterNames(const string &indexPath, 
                                    vector<string> &clusterNames) 
{
    FileList fileList;
    ErrorCode errorCode = FileSystem::listDir(indexPath, fileList);
    if (EC_OK != errorCode) {
        HA3_LOG(ERROR, "Failed to list index dir:[%s], errorMsg:[%s]",
                indexPath.c_str(), FileSystem::getErrorString(errorCode).c_str());
        return false;
    }
    clusterNames.clear();
    for(FileList::const_iterator it = fileList.begin(); it != fileList.end(); ++it) {
        assert( !(*it == "." || *it == ".."));
        string clusterIndexPath = FileUtil::joinFilePath(indexPath, *it);
        ErrorCode ec = FileSystem::isDirectory(clusterIndexPath);
        if (EC_TRUE == ec) {
            clusterNames.push_back(*it);
        } else if (EC_FALSE != ec) {
             HA3_LOG(ERROR, "Failed to check cluster index path[%s], errorMsg[%s]",
                     clusterIndexPath.c_str(), FileSystem::getErrorString(ec).c_str());
            return false;
        }
    }
    return true;
}

bool IndexScanner::scanCluster(const string &clusterIndexPath,
                               ClusterIndexInfo &clusterIndexInfo, 
                               bool isOffline,
                               uint32_t offlineLevel)
{
    FileList fileList;
    ErrorCode errorCode = FileSystem::listDir(clusterIndexPath, fileList);
    if (EC_OK != errorCode) {
        HA3_LOG(ERROR, "Failed to list cluster index dir:[%s], errorMsg:[%s]",
                clusterIndexPath.c_str(), FileSystem::getErrorString(errorCode).c_str());
        return false;
    }
    
    vector<uint32_t> generationIdVec;
    HaPathDefs::sortGenerationList(fileList, generationIdVec);

    bool ret = true;
    MultiLevelRangeSet rangeSets(offlineLevel);
    for(size_t i = 0; i < fileList.size(); ++i) {
        uint32_t generationId = generationIdVec[i];
        string generationName = fileList[i];
        assert(generationName != "." && generationName != "..");
        string generationPath = FileUtil::joinFilePath(clusterIndexPath, generationName);
        
        errorCode = FileSystem::isDirectory(generationPath);
        if (EC_TRUE == errorCode) {
            if (!isGenerationValid(generationPath)) {
                continue;
            }
            GenerationInfo generationInfo;
            if (!scanGeneration(generationPath, generationInfo, rangeSets,
                            isOffline)) 
            {
                ret = false;
                continue;
            }
            clusterIndexInfo[generationId] = generationInfo;
            if (ret && isOffline && rangeSets.isComplete()) {
                return true;
            }
        } else if (EC_FALSE != errorCode) {
            HA3_LOG(ERROR, "Failed to judge [%s] isDirectory. errorMsg:[%s]",
                    generationPath.c_str(),
                    FileSystem::getErrorString(errorCode).c_str());
            ret = false;
        }
    }
    return ret;
}

bool IndexScanner::scanGeneration(const string &generationIndexPath,
                                  GenerationInfo &generationInfo,
                                  MultiLevelRangeSet& rangeSets,
                                  bool isOffline)
{
    FileList fileList;
    ErrorCode errorCode = FileSystem::listDir(generationIndexPath, fileList);
    if (EC_OK != errorCode) {
        HA3_LOG(ERROR, "Failed to list generation dir:[%s], errorMsg:[%s]",
                generationIndexPath.c_str(),
                FileSystem::getErrorString(errorCode).c_str());
        return false;
    }

    for (FileList::iterator it = fileList.begin(); it != fileList.end(); ++it) {
        string partitionPath = FileUtil::joinFilePath(generationIndexPath,(*it));
        assert((*it) != "." && (*it) != "..");
        uint16_t from;
        uint16_t to;
        if (HaPathDefs::dirNameToRange((*it), from, to) 
            && ((!isOffline)|| (isOffline && rangeSets.tryPush(from, to)))
            && EC_TRUE == FileSystem::isDirectory(partitionPath))
        {
            bool hasError = false;
            uint32_t incrementalVersion;
            if (getMaxIncrementalVersion(partitionPath, incrementalVersion, hasError)) {
                Range range;
                range.set_from(from);
                range.set_to(to);
                generationInfo[range] = incrementalVersion;
                if (isOffline) {
                    rangeSets.push(from, to);
                    if (rangeSets.isComplete()) {
                        return true;
                    }
                }
            }
            if (hasError) {
                return false;
            }
        }
    }
    return true;
}

bool IndexScanner::getMaxIncrementalVersion(const string &partitionPath,
        uint32_t &version, bool &hasError)
{
    hasError = false;
    FileList fileList; 
    ErrorCode ec = FileSystem::listDir(partitionPath, fileList);
    if (EC_OK != ec) {
        hasError = (ec != EC_NOENT);
        HA3_LOG(ERROR, "Failed to list partition dir:[%s], errorMsg:[%s]",
                partitionPath.c_str(), FileSystem::getErrorString(ec).c_str());
        return false;
    }

    version = 0;
    string maxVersionName = "";
    for (FileList::iterator it = fileList.begin(); it != fileList.end(); ++it) {
        uint32_t tempVersion = 0;
        if (!HaPathDefs::fileNameToVersion(*it, tempVersion)) {
            continue;
        }
        if (tempVersion >= version) {
            version = tempVersion;
            maxVersionName = *it;
        }
    }
    if (!maxVersionName.empty()) {
        string versionPath = FileUtil::joinFilePath(partitionPath, maxVersionName);
        ErrorCode ec = FileSystem::isFile(versionPath);
        if (EC_TRUE == ec) {
            return true; 
        } else if (EC_FALSE == ec) {
            hasError = true;
            HA3_LOG(ERROR, "Get max incremental version failed, not a file [%s]",
                    versionPath.c_str());
            return false;
        } else {
            hasError = (ec != EC_NOENT);
            HA3_LOG(ERROR, "check isFile [%s] causes exception",
                    versionPath.c_str());
            return false;
        }
    } else {
        return false;
    }
}

bool IndexScanner::isGenerationValid(const string& generationPath) {
    string invalidFlagFile = FileUtil::joinFilePath(generationPath,
            INDEX_INVALID_FLAG);
    ErrorCode errorCode = FileSystem::isExist(invalidFlagFile);
    if (errorCode == EC_TRUE) {
        HA3_LOG(INFO, "generation[%s] is invalid, skip it",
                generationPath.c_str());
        return false;
    } else if (errorCode == EC_FALSE) {
        return true;
    } else {
        HA3_LOG(ERROR, "check [%s] exist failed, skip this generation",
                invalidFlagFile.c_str());
        return false;
    }
}

END_HA3_NAMESPACE(common);

