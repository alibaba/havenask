/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/workflow/RealtimeBuilder.h"

#include <cstddef>
#include <map>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Span.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/builder/Builder.h"
#include "build_service/common/ConfigDownloader.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/workflow/ProcessedDocRtBuilderImpl.h"
#include "build_service/workflow/RawDocRtJobBuilderImpl.h"
#include "build_service/workflow/RawDocRtServiceBuilderImpl.h"
#include "build_service/workflow/RealtimeBuilderImplBase.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/index_base/common_branch_hinter_option.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/partition_group_resource.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::common;

using namespace indexlib::index_base;
using namespace indexlib::partition;

namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, RealtimeBuilder);

RealtimeBuilder::RealtimeBuilder(const string& configPath, const IndexPartitionPtr& indexPart,
                                 const RealtimeBuilderResource& builderResource)
    : _impl(NULL)
    , _configPath(configPath)
    , _indexPartition(indexPart)
    , _builderResource(builderResource)
{
}

RealtimeBuilder::~RealtimeBuilder() { DELETE_AND_SET_NULL(_impl); }

bool RealtimeBuilder::start(const proto::PartitionId& partitionId)
{
    if (!_impl) {
        _impl = createRealtimeBuilderImpl(partitionId);
    }
    if (!_impl) {
        return false;
    }
    return _impl->start(partitionId, _kvMap);
}

void RealtimeBuilder::stop()
{
    if (_impl) {
        _impl->stop();
    }
}

void RealtimeBuilder::suspendBuild() { return _impl->suspendBuild(); }

void RealtimeBuilder::resumeBuild() { return _impl->resumeBuild(); }

bool RealtimeBuilder::isRecovered() { return _impl->isRecovered(); }

void RealtimeBuilder::forceRecover() { _impl->forceRecover(); }

void RealtimeBuilder::setTimestampToSkip(int64_t timestamp) { _impl->setTimestampToSkip(timestamp); }

void RealtimeBuilder::getErrorInfo(RealtimeErrorCode& errorCode, string& errorMsg, int64_t& errorTime) const
{
    return _impl->getErrorInfo(errorCode, errorMsg, errorTime);
}

RealtimeBuilderImplBase* RealtimeBuilder::createRealtimeBuilderImpl(const proto::PartitionId& partitionId)
{
    KeyValueMap kvMap;
    if (!getRealtimeInfo(&kvMap)) {
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

bool RealtimeBuilder::downloadConfig(const string& indexRoot, KeyValueMap* kvMap)
{
    string localConfigPath = fslib::util::FileUtil::joinFilePath(indexRoot, "config");
    string remoteConfigPath = (*kvMap)[CONFIG_PATH];
    ConfigDownloader::DownloadErrorCode errorCode = ConfigDownloader::downloadConfig(remoteConfigPath, localConfigPath);
    if (errorCode == ConfigDownloader::DEC_NORMAL_ERROR || errorCode == ConfigDownloader::DEC_DEST_ERROR) {
        BS_LOG(ERROR, "downloadConfig from %s failed", remoteConfigPath.c_str());
        return false;
    }
    (*kvMap)[CONFIG_PATH] = localConfigPath;
    return true;
}

std::string RealtimeBuilder::getIndexRoot() const { return _indexPartition->GetSecondaryRootPath(); }

bool RealtimeBuilder::getRealtimeInfo(KeyValueMap* kvMap)
{
    if (_builderResource.realtimeInfo.isValid()) {
        *kvMap = _builderResource.realtimeInfo.getKvMap();
        auto indexRoot = getIndexRoot();
        if ((*kvMap)[REALTIME_MODE] == REALTIME_JOB_MODE && !downloadConfig(indexRoot, kvMap)) {
            BS_LOG(ERROR, "download config from %s to %s failed.", (*kvMap)[CONFIG_PATH].c_str(), indexRoot.c_str());
            return false;
        } else if ((*kvMap)[REALTIME_MODE] == REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE) {
            if (_configPath.empty()) {
                BS_LOG(ERROR, "_configPath is empty");
                return false;
            }
            (*kvMap)[CONFIG_PATH] = _configPath;
        }
        BS_LOG(INFO, "parse realtime_info from file success.");
    } else {
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
        BuildServiceConfig serviceConfig;
        if (!resourceReader->getConfigWithJsonPath("build_app.json", "", serviceConfig)) {
            // todo: set error info
            string errorMsg = "fail to parse build_app.json";
            BS_LOG(ERROR, "%s, configPath[%s]", errorMsg.c_str(), _configPath.c_str());
            return false;
        }
        (*kvMap)[PROCESSED_DOC_SWIFT_ROOT] = serviceConfig.getSwiftRoot(false);
        (*kvMap)[PROCESSED_DOC_SWIFT_TOPIC_PREFIX] = serviceConfig.getApplicationId();
        (*kvMap)[REALTIME_MODE] = REALTIME_SERVICE_MODE;
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

bool RealtimeBuilder::loadRealtimeInfo(const std::string& root, RealtimeInfoWrapper* realtimeInfo)
{
    return realtimeInfo->load(root);
}

}} // namespace build_service::workflow
