#include "build_service/workflow/RealtimeBuilder.h"
#include "build_service/workflow/ProcessedDocRtBuilderImpl.h"
#include "build_service/workflow/RawDocRtJobBuilderImpl.h"
#include "build_service/workflow/RawDocRtServiceBuilderImpl.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/util/FileUtil.h"
#include "build_service/task_base/ConfigDownloader.h"
#include <indexlib/file_system/directory.h>
#include <indexlib/partition/index_partition.h>
#include <autil/legacy/jsonizable.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::task_base;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(partition);

namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, RealtimeBuilder);

RealtimeBuilder::RealtimeBuilder(const string &configPath,
                                 const IndexPartitionPtr &indexPart,
                                 const RealtimeBuilderResource& builderResource)
    : _impl(NULL)
    , _configPath(configPath)
    , _indexPartition(indexPart)
    , _builderResource(builderResource)
{
}

RealtimeBuilder::~RealtimeBuilder() {
    DELETE_AND_SET_NULL(_impl);
}

bool RealtimeBuilder::start(const proto::PartitionId &partitionId) {
    if (!_impl) {
        _impl = createRealtimeBuilderImpl(partitionId);
    }
    if (!_impl) {
        return false;
    }
    return _impl->start(partitionId, _kvMap);
}

void RealtimeBuilder::stop() {
    _impl->stop();
}

void RealtimeBuilder::suspendBuild() {
    return _impl->suspendBuild();
}

void RealtimeBuilder::resumeBuild() {
    return _impl->resumeBuild();
}

bool RealtimeBuilder::isRecovered() {
    return _impl->isRecovered();
}

void RealtimeBuilder::forceRecover() {
    _impl->forceRecover();
}

void RealtimeBuilder::setTimestampToSkip(int64_t timestamp) {
    _impl->setTimestampToSkip(timestamp);
}

void RealtimeBuilder::getErrorInfo(RealtimeErrorCode &errorCode,
                                   string &errorMsg, int64_t &errorTime) const
{
    return _impl->getErrorInfo(errorCode, errorMsg, errorTime);
}

RealtimeBuilderImplBase *RealtimeBuilder::createRealtimeBuilderImpl(
        const proto::PartitionId &partitionId)
{
    KeyValueMap kvMap;
    if (!getRealtimeInfo(kvMap)) {
        BS_LOG(ERROR, "[%s] get realtime info failed, can not create readltime builder impl",
               partitionId.buildid().ShortDebugString().c_str());
        return NULL;
    }
    _kvMap = kvMap;
    
    if (_kvMap[REALTIME_MODE] == REALTIME_SERVICE_MODE) {
        return new ProcessedDocRtBuilderImpl(_configPath, _indexPartition, _builderResource);
    }
    if (_kvMap[REALTIME_MODE] == REALTIME_JOB_MODE) {
        return new RawDocRtJobBuilderImpl(_configPath, _indexPartition, _builderResource);
    }
    if (_kvMap[REALTIME_MODE] == REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE) {
        return new RawDocRtServiceBuilderImpl(_configPath, _indexPartition, _builderResource);
    }    
    BS_LOG(ERROR, "[%s] invalid realtime mode, can not create realtime builder impl",
           partitionId.buildid().ShortDebugString().c_str());
    return NULL;
}

string RealtimeBuilder::getIndexRoot() const {
    return _indexPartition->GetSecondaryRootPath();
}

bool RealtimeBuilder::parseRealtimeInfo(const string &realtimeInfo, KeyValueMap &kvMap) {
    json::JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, realtimeInfo);
    } catch (const ExceptionBase &e) {
        string errorMsg = "jsonize " + realtimeInfo 
                          + " failed, exception: " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (auto iter:jsonMap) {
        const string &key = iter.first;
        string value;
        try {
            FromJson(value, iter.second);
        } catch (const autil::legacy::ExceptionBase &e) {
            BS_LOG(ERROR, "fail to parse json map[%s].", ToJsonString(jsonMap).c_str());
            return false;
        }
        kvMap[key] = value;
    }
    return true;
}

bool RealtimeBuilder::downloadConfig(const string &indexRoot, KeyValueMap &kvMap) {
    string localConfigPath = FileUtil::joinFilePath(indexRoot, "config");
    string remoteConfigPath = kvMap[CONFIG_PATH];
    ConfigDownloader::DownloadErrorCode errorCode =
        ConfigDownloader::downloadConfig(remoteConfigPath, localConfigPath);
    if (errorCode == ConfigDownloader::DEC_NORMAL_ERROR
        || errorCode == ConfigDownloader::DEC_DEST_ERROR)
    {
        BS_LOG(ERROR, "downloadConfig from %s failed", remoteConfigPath.c_str());
        return false;
    }
    kvMap[CONFIG_PATH] = localConfigPath;
    return true;
}

bool RealtimeBuilder::getRealtimeInfo(KeyValueMap &kvMap)
{
    const string &indexRoot = getIndexRoot();
    if (indexRoot.empty()) {
        BS_LOG(ERROR, "failed to get path from directory");
        return false;
    }

    const string &generationPath = FileUtil::getParentDir(indexRoot);
    string realtimeInfoFile = FileUtil::joinFilePath(generationPath, REALTIME_INFO_FILE_NAME);
    bool fileExist = false;
    if (!FileUtil::isFile(realtimeInfoFile, fileExist)) {
        BS_LOG(ERROR, "check whether [%s] is file failed.", realtimeInfoFile.c_str());
        return false;
    }
    if (fileExist) {
        string fileContent;
        if (!FileUtil::readFile(realtimeInfoFile, fileContent)) {
            BS_LOG(ERROR, "fail to read %s", realtimeInfoFile.c_str());
            return false;
        }
        if (!parseRealtimeInfo(fileContent, kvMap)) {
            BS_LOG(ERROR, "fail to parse realtime info from [%s]", fileContent.c_str());
            return false;
        }
        if (kvMap[REALTIME_MODE] == REALTIME_JOB_MODE &&
            !downloadConfig(indexRoot, kvMap))
        {
            BS_LOG(ERROR, "download config from %s to %s failed.",
                   kvMap[CONFIG_PATH].c_str(), indexRoot.c_str());
            return false;
        }
        if (kvMap[REALTIME_MODE] == REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE) {
            if (_configPath.empty()) {
                BS_LOG(ERROR, "_configPath is empty");
                return false;
            }
            kvMap[CONFIG_PATH] = _configPath; 
        }
        BS_LOG(INFO, "parse realtime_info_file [%s], content [%s] success.",
               realtimeInfoFile.c_str(), fileContent.c_str());
    } else {
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
        BuildServiceConfig serviceConfig;
        if (!resourceReader->getConfigWithJsonPath("build_app.json",
                        "", serviceConfig))
        {
            // todo: set error info
            string errorMsg = "fail to parse build_app.json";
            BS_LOG(ERROR, "%s, configPath[%s]", errorMsg.c_str(), _configPath.c_str());
            return false;
        }
        kvMap[PROCESSED_DOC_SWIFT_ROOT] = serviceConfig.swiftRoot;
        kvMap[PROCESSED_DOC_SWIFT_TOPIC_PREFIX] = serviceConfig.getApplicationId();
        kvMap[REALTIME_MODE] = REALTIME_SERVICE_MODE;
        BS_LOG(INFO, "parse build_app.json in [%s] success.", _configPath.c_str());
    }
    return true;
}

builder::Builder* RealtimeBuilder::GetBuilder() const
{
    if (!_impl) {
        return nullptr;
    }
    return _impl->GetBuilder();
}

}
}
