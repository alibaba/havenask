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
#include "build_service/workflow/RealtimeBuilderV2.h"

#include <assert.h>
#include <chrono>
#include <map>
#include <type_traits>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/common/ConfigDownloader.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/workflow/DocumentBatchRtBuilderImplV2.h"
#include "build_service/workflow/ProcessedDocRtBuilderImplV2.h"
#include "build_service/workflow/RawDocRtServiceBuilderImplV2.h"
#include "build_service/workflow/StopOption.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/CommitOptions.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/TabletInfos.h"

namespace build_service::workflow {

BS_LOG_SETUP(workflow, RealtimeBuilderV2);

RealtimeBuilderV2::RealtimeBuilderV2(const std::string configPath,
                                     std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                                     RealtimeBuilderResource builderResource)
    : _configPath(std::move(configPath))
    , _tablet(std::move(tablet))
    , _builderResource(std::move(builderResource))
{
    std::string tabletName;
    if (_tablet) {
        tabletName = _tablet->GetTabletInfos()->GetTabletName();
    }
    _tasker = std::make_unique<future_lite::NamedTaskScheduler>(builderResource.taskScheduler2);
}

bool RealtimeBuilderV2::start(const proto::PartitionId& partitionId)
{
    KeyValueMap kvMap;
    if (!getRealtimeInfo(partitionId, &kvMap)) {
        BS_LOG(ERROR, "get realtime info for generation [%u] failed, config path [%s]",
               partitionId.buildid().generationid(), _configPath.c_str());
        return false;
    }
    _schemaListKeeper.reset(new config::RealtimeSchemaListKeeper);
    assert(partitionId.clusternames_size() == 1);
    const std::string& clusterName = partitionId.clusternames(0);
    std::string generationPath = fslib::util::FileUtil::getParentDir(_builderResource.rawIndexRoot);
    std::string clusterPath = fslib::util::FileUtil::getParentDir(generationPath);
    std::string indexPath = fslib::util::FileUtil::getParentDir(clusterPath);
    _schemaListKeeper->init(indexPath, clusterName, partitionId.buildid().generationid());
    std::lock_guard<std::mutex> lock(_implLock);
    if (!_impl) {
        _impl.reset(createRealtimeBuilderImpl(partitionId, kvMap));
    }
    if (!_impl) {
        BS_LOG(ERROR, "create realtime builder impl for generation [%u] failed", partitionId.buildid().generationid());
        return false;
    }

    using namespace std::chrono_literals;
    if (!_tasker->HasTask("check_and_reconstruct")) {
        bool ret = _tasker->StartIntervalTask(
            "check_and_reconstruct", [this, partitionId]() mutable { checkAndReconstruct(partitionId); }, 1s);
        if (!ret) {
            BS_LOG(ERROR, "create checkAndReconstruct task failed");
            return false;
        }
    }
    return _impl->start(partitionId, kvMap);
}

void RealtimeBuilderV2::stop()
{
    std::lock_guard<std::mutex> lock(_implLock);
    _tasker->DeleteTask("check_and_reconstruct");
    if (_impl) {
        _impl->stop(SO_INSTANT);
    }
}

void RealtimeBuilderV2::checkAndReconstruct(const proto::PartitionId& partitionId)
{
    std::unique_lock<std::mutex> lock(_implLock, std::defer_lock);
    if (!lock.try_lock()) {
        return;
    }
    if (_impl && _impl->needReconstruct()) {
        _impl->stop(SO_INSTANT);
        _impl.reset();
        lock.unlock();
        start(partitionId);
    } else {
        if (_impl && _impl->needAlterTable()) {
            if (!_schemaListKeeper->updateSchemaCache()) {
                BS_LOG(ERROR, "update schema list failed");
                return;
            }
            auto nextSchemaId = _impl->getAlterTableSchemaId();
            std::vector<std::shared_ptr<indexlibv2::config::TabletSchema>> schemaList;
            if (!_schemaListKeeper->getSchemaList(_tablet->GetTabletSchema()->GetSchemaId() + 1, nextSchemaId,
                                                  schemaList)) {
                BS_LOG(ERROR, "get schema list failed");
                return;
            }
            if (schemaList.size() == 0) {
                BS_LOG(ERROR, "get schema list empty");
                return;
            }
            for (auto schema : schemaList) {
                auto status = _tablet->AlterTable(schema);
                if (!status.IsOK()) {
                    return;
                }
            }
            _impl->stop(SO_INSTANT);
            _impl.reset();
            lock.unlock();
            start(partitionId);
        }
    }
}

void RealtimeBuilderV2::suspendBuild()
{
    std::lock_guard<std::mutex> lock(_implLock);
    if (_impl) {
        return _impl->suspendBuild();
    }
}

void RealtimeBuilderV2::resumeBuild()
{
    std::lock_guard<std::mutex> lock(_implLock);
    if (_impl) {
        return _impl->resumeBuild();
    }
}

bool RealtimeBuilderV2::isRecovered()
{
    std::lock_guard<std::mutex> lock(_implLock);
    if (_impl) {
        return _impl->isRecovered();
    }
    return false;
}

void RealtimeBuilderV2::forceRecover()
{
    std::lock_guard<std::mutex> lock(_implLock);
    if (_impl) {
        _impl->forceRecover();
    }
}

void RealtimeBuilderV2::setTimestampToSkip(int64_t timestamp)
{
    std::lock_guard<std::mutex> lock(_implLock);
    if (_impl) {
        _impl->setTimestampToSkip(timestamp);
    }
}

void RealtimeBuilderV2::getErrorInfo(RealtimeErrorCode& errorCode, std::string& errorMsg, int64_t& errorTime) const
{
    std::lock_guard<std::mutex> lock(_implLock);
    if (_impl) {
        return _impl->getErrorInfo(errorCode, errorMsg, errorTime);
    }
}

bool RealtimeBuilderV2::needCommit() const
{
    if (_tablet) {
        return _tablet->NeedCommit();
    }
    return false;
}

std::pair<indexlib::Status, indexlibv2::framework::VersionMeta> RealtimeBuilderV2::commit()
{
    if (_tablet) {
        bool needReopen = _tablet->GetTabletOptions()->FlushLocal();
        auto commitOptions =
            indexlibv2::framework::CommitOptions().SetNeedPublish(true).SetNeedReopenInCommit(needReopen);
        if (_impl) {
            commitOptions.AddVersionDescription(
                "generation", autil::StringUtil::toString(_impl->getPartitionId().buildid().generationid()));
        }
        return _tablet->Commit(commitOptions);
    }
    return std::make_pair(indexlib::Status::Corruption("no impl in realtime builder"),
                          indexlibv2::framework::VersionMeta());
}

bool RealtimeBuilderV2::isFinished() const
{
    std::lock_guard<std::mutex> lock(_implLock);
    if (_impl) {
        return _impl->isFinished();
    }
    return false;
}

std::string RealtimeBuilderV2::getIndexRoot() const
{
    return std::string(_tablet->GetTabletInfos()->GetIndexRoot().GetRemoteRoot());
}

bool RealtimeBuilderV2::downloadConfig(const std::string& indexRoot, KeyValueMap* kvMap) const
{
    std::string localConfigPath = fslib::util::FileUtil::joinFilePath(indexRoot, "config");
    std::string remoteConfigPath = (*kvMap)[config::CONFIG_PATH];
    common::ConfigDownloader::DownloadErrorCode errorCode =
        common::ConfigDownloader::downloadConfig(remoteConfigPath, localConfigPath);
    if (errorCode == common::ConfigDownloader::DEC_NORMAL_ERROR ||
        errorCode == common::ConfigDownloader::DEC_DEST_ERROR) {
        return false;
    }
    (*kvMap)[config::CONFIG_PATH] = localConfigPath;
    return true;
}

bool RealtimeBuilderV2::getRealtimeInfo(const proto::PartitionId& partitionId, KeyValueMap* kvMap)
{
    if (_builderResource.realtimeInfo.isValid()) {
        *kvMap = _builderResource.realtimeInfo.getKvMap();
        std::string indexRoot = getIndexRoot(); // ? download to remote path
        if ((*kvMap)[config::REALTIME_MODE] == config::REALTIME_JOB_MODE && !downloadConfig(indexRoot, kvMap)) {
            BS_LOG(ERROR, "download config from %s to %s failed.", (*kvMap)[config::CONFIG_PATH].c_str(),
                   indexRoot.c_str());
            return false;
        } else if ((*kvMap)[config::REALTIME_MODE] == config::REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE) {
            if (_configPath.empty()) {
                BS_LOG(ERROR, "_configPath is empty");
                return false;
            }
            (*kvMap)[config::CONFIG_PATH] = _configPath;
        }
        BS_LOG(INFO, "parse realtime_info from file success.");
    } else {
        config::ResourceReaderPtr resourceReader = config::ResourceReaderManager::getResourceReader(_configPath);
        config::ControlConfig controlConfig;
        std::string dataTableName = partitionId.buildid().datatable();
        bool ret = resourceReader->getDataTableConfigWithJsonPath(dataTableName, "control_config", controlConfig);
        assert(partitionId.clusternames_size() == 1);
        const std::string& clusterName = partitionId.clusternames(0);
        if (ret && !controlConfig.isIncProcessorExist(clusterName)) {
            return getRealtimeInfoFromDataDescription(dataTableName, *resourceReader, kvMap);
        }

        config::BuildServiceConfig serviceConfig;
        if (!resourceReader->getConfigWithJsonPath("build_app.json", "", serviceConfig)) {
            // todo: set error info
            std::string errorMsg = "fail to parse build_app.json";
            BS_LOG(ERROR, "%s, configPath[%s]", errorMsg.c_str(), _configPath.c_str());
            return false;
        }
        (*kvMap)[config::PROCESSED_DOC_SWIFT_ROOT] = serviceConfig.getSwiftRoot(false);
        (*kvMap)[config::PROCESSED_DOC_SWIFT_TOPIC_PREFIX] = serviceConfig.getApplicationId();
        (*kvMap)[config::REALTIME_MODE] = config::REALTIME_SERVICE_MODE;
        _builderResource.realtimeInfo = RealtimeInfoWrapper(*kvMap);
        BS_LOG(INFO, "parse build_app.json in [%s] success.", _configPath.c_str());
    }
    return true;
}

bool RealtimeBuilderV2::getRealtimeInfoFromDataDescription(const std::string& dataTableName,
                                                           config::ResourceReader& resourceReader, KeyValueMap* kvMap)
{
    proto::DataDescriptions dataDescs;
    if (!resourceReader.getDataTableConfigWithJsonPath(dataTableName, "data_descriptions", dataDescs.toVector())) {
        BS_LOG(ERROR, "invalid data_descriptions in %s_table.json", dataTableName.c_str());
        return false;
    }
    if (dataDescs.size() == 0) {
        BS_LOG(ERROR, "data_descriptions is empty");
        return false;
    }
    if (dataDescs.size() != 1) {
        BS_LOG(ERROR, "not support multi data source");
        return false;
    }
    const auto& desc = dataDescs[dataDescs.size() - 1];
    for (auto& [key, value] : desc) {
        if (kvMap->find(key) == kvMap->end()) {
            (*kvMap)[key] = value;
        }
    }
    (*kvMap)[config::REALTIME_MODE] = config::REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE;
    (*kvMap)[config::CONFIG_PATH] = _configPath;
    (*kvMap)[config::SRC_SIGNATURE] = autil::StringUtil::toString(dataDescs.size() - 1);
    _builderResource.realtimeInfo = RealtimeInfoWrapper(*kvMap);
    return true;
}

RealtimeBuilderImplV2* RealtimeBuilderV2::createRealtimeBuilderImpl(const proto::PartitionId& partitionId,
                                                                    const KeyValueMap& kvMap) const
{
    auto iter = kvMap.find(config::REALTIME_MODE);
    if (iter == kvMap.end()) {
        BS_LOG(ERROR, "[%s] null realtime mode", partitionId.buildid().ShortDebugString().c_str());
        return nullptr;
    }
    // TODO(xiaohao.yxh) add ProcessedDocRtBuilderImplV2
    if (iter->second == config::REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE) {
        return new RawDocRtServiceBuilderImplV2(_configPath, _tablet, _builderResource, _tasker.get());
    }
    if (iter->second == config::REALTIME_SERVICE_MODE) {
        return new ProcessedDocRtBuilderImplV2(_configPath, _tablet, _builderResource, _tasker.get());
    }
    if (iter->second == config::REALTIME_SERVICE_NPC_MODE) {
        return new DocumentBatchRtBuilderImplV2(_configPath, _tablet, _builderResource, _tasker.get());
    }
    assert(false);
    // TODO(hanyao): add service mode and job mode
    BS_LOG(ERROR, "[%s] invalid realtime mode, can not create realtime builder impl",
           partitionId.buildid().ShortDebugString().c_str());
    return nullptr;
}

} // namespace build_service::workflow
