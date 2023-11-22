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
#include "suez/table/PartitionProperties.h"

#include "autil/Log.h"
#include "build_service/workflow/RealtimeBuilder.h"
#include "fslib/fs/FileSystem.h"
#include "resource_reader/ResourceReader.h"
#include "suez/common/TablePathDefine.h"
#include "suez/sdk/PathDefine.h"

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, PartitionProperties);

void TableConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("realtime", _realtime, _realtime);
    json.Jsonize("disable_follower_build", _disableFollowerBuild, _disableFollowerBuild);
    json.Jsonize("direct_write", _directWrite, _directWrite);
    json.Jsonize("async_direct_write", _asyncDirectWrite, _asyncDirectWrite);
    if (json.GetMode() == TO_JSON) {
        json.Jsonize("merge_controller", _mergeControllerConfig, _mergeControllerConfig);
    } else {
        auto jsonMap = json.GetMap();
        auto iter = jsonMap.find("merge_controller");
        if (iter != jsonMap.end()) {
            json.Jsonize("merge_controller", _mergeControllerConfig, _mergeControllerConfig);
            _hasMergeControllerConfig = true;
        } else {
            _hasMergeControllerConfig = false;
            _mergeControllerConfig.mode = "local";
        }
    }
    json.Jsonize("drc_config", _drcConfig, _drcConfig);
    json.Jsonize("wal_config", _walConfig, _walConfig);
    json.Jsonize("realtime_info", _realtimeInfo, _realtimeInfo);
    json.Jsonize("data_link_mode", _dataLinkMode, _dataLinkMode);
}

bool TableConfig::loadFromFile(const std::string &path, const std::string &fileName, bool isIndexPartition) {
    resource_reader::ResourceReader resourceReader(path);

    std::string configContent;
    if (!resourceReader.getConfigContent(fileName, configContent)) {
        AUTIL_LOG(ERROR, "read config from %s / %s failed", path.c_str(), fileName.c_str());
        return false;
    }

    try {
        autil::legacy::FastFromJsonString(*this, configContent);
        autil::legacy::FastFromJsonString(_tabletOptions, configContent);
        autil::legacy::FastFromJsonString(_tableDefConfig, configContent);
    } catch (const std::exception &e) {
        AUTIL_LOG(ERROR, "parse table config from %s failed, error: %s", configContent.c_str(), e.what());
        return false;
    }
    _tabletOptions.SetIsLegacy(isIndexPartition);
    return true;
}

PartitionProperties::PartitionProperties(const PartitionId &pid) : _pid(pid) {}

PartitionProperties::~PartitionProperties() {}

bool PartitionProperties::init(const TargetPartitionMeta &target, const TableConfig &tableConfig) {
    this->roleType = target.getRoleType();
    this->version = target.getIncVersion();
    this->localConfigPath = TablePathDefine::constructLocalConfigPath(_pid.getTableName(), target.getConfigPath());
    this->remoteConfigPath = target.getConfigPath();

    this->hasRt = tableConfig.hasRealtime();
    this->disableFollowerBuild = tableConfig.disableFollowerBuild();
    this->allowFollowerWrite = autil::EnvUtil::getEnv("ALLOW_FOLLOWER_WRITE", false);
    this->directWrite = tableConfig.directWrite();
    this->asyncDirectWrite = tableConfig.asyncDirectWrite();
    const auto &drcConfig = tableConfig.getDrcConfig();
    if (drcConfig.isEnabled()) {
        this->drcConfig = drcConfig;
    } else {
        this->drcConfig.setEnabled(false);
    }
    this->tabletOptions = tableConfig.getTabletOptions();
    this->mergeControllerConfig = tableConfig.getMergeControllerConfig();
    this->walConfig = tableConfig.getWALConfig();
    if (!this->tabletOptions.IsRawJsonEmpty() &&
        !this->tabletOptions.GetFromRawJson("", &(this->indexOptions)).IsOK()) {
        AUTIL_LOG(
            ERROR, "rejsonize to IndexPartitionOptions for %s failed", autil::legacy::FastToJsonString(_pid).c_str());
        return false;
    }

    this->primaryIndexRoot = TablePathDefine::constructIndexPath(PathDefine::getLocalIndexRoot(), _pid);
    this->secondaryIndexRoot = TablePathDefine::constructIndexPath(target.getIndexRoot(), _pid);
    this->rawIndexRoot = TablePathDefine::constructIndexPath(target.getRawIndexRoot(), _pid);
    this->schemaRoot = tabletOptions.GetNeedReadRemoteIndex() ? this->secondaryIndexRoot : this->primaryIndexRoot;

    if (this->hasRt || this->directWrite) {
        if (!loadRealtimeInfo(_pid, tableConfig, schemaRoot, &realtimeInfo)) {
            AUTIL_LOG(ERROR, "load realtime info for %s failed", autil::legacy::FastToJsonString(_pid).c_str());
            return false;
        }
        if (tableConfig.getRealtimeInfo().empty()) {
            if (!tableConfig.hasMergeControllerConfig() && !realtimeInfo.getBsServerAddress().empty()) {
                // when no merge controller config && has remote bs service, use remote mode
                this->mergeControllerConfig.mode = "remote";
            }
        }
    }
    return true;
}

bool PartitionProperties::mergeConfig(const std::map<std::string, std::string> &srcMap,
                                      const std::string &key,
                                      std::map<std::string, std::string> &dstMap) {
    auto srcIter = srcMap.find(key);
    if (srcIter == srcMap.end()) {
        return true;
    }
    auto dstIter = dstMap.find(key);
    if (dstIter != dstMap.end()) {
        if (srcIter->second != dstIter->second) {
            return false;
        }
        return true;
    } else {
        dstMap[key] = srcIter->second;
        return true;
    }
}

bool PartitionProperties::loadRealtimeInfo(const PartitionId &pid,
                                           const std::string &remoteConfigPath,
                                           const std::string &indexRoot,
                                           build_service::workflow::RealtimeInfoWrapper *realtimeInfo) {
    std::string configPath = TablePathDefine::constructLocalConfigPath(pid.getTableName(), remoteConfigPath);
    if (fslib::EC_TRUE != fslib::fs::FileSystem::isExist(configPath)) {
        configPath = remoteConfigPath;
    }
    TableConfig tableConfig;
    const auto &fileName = PathDefine::getTableConfigFileName(pid.getTableName());
    if (!tableConfig.loadFromFile(configPath, fileName, false)) {
        AUTIL_LOG(ERROR, "load table config from file failed, [%s]", fileName.c_str());
        return false;
    }
    return loadRealtimeInfo(pid, tableConfig, indexRoot, realtimeInfo);
}

bool PartitionProperties::loadRealtimeInfo(const PartitionId &pid,
                                           const TableConfig &tableConfig,
                                           const std::string &indexRoot,
                                           build_service::workflow::RealtimeInfoWrapper *realtimeInfo) {
    build_service::workflow::RealtimeInfoWrapper realtimeInfoFromIndex;
    if (!realtimeInfoFromIndex.load(indexRoot)) {
        AUTIL_LOG(ERROR,
                  "load realtime info from %s for %s failed",
                  indexRoot.c_str(),
                  autil::legacy::FastToJsonString(pid).c_str());
        return false;
    }
    auto realtimeInfoFromConfig = tableConfig.getRealtimeInfo();
    if (!realtimeInfoFromConfig.empty()) {
        // merge 'app_name' and 'data_table' from realtimeInfoFromIndex
        if (!mergeConfig(realtimeInfoFromIndex.getKvMap(), build_service::config::APP_NAME, realtimeInfoFromConfig)) {
            AUTIL_LOG(ERROR, "%s in config and index is not same", build_service::config::APP_NAME.c_str());
            return false;
        }
        if (!mergeConfig(
                realtimeInfoFromIndex.getKvMap(), build_service::config::DATA_TABLE_NAME, realtimeInfoFromConfig)) {
            AUTIL_LOG(ERROR, "%s in config and index is not same", build_service::config::DATA_TABLE_NAME.c_str());
            return false;
        }
        // TODO: validate
        *realtimeInfo = build_service::workflow::RealtimeInfoWrapper(realtimeInfoFromConfig);
        AUTIL_LOG(DEBUG, "construct realtime info from table config");
    } else {
        *realtimeInfo = realtimeInfoFromIndex;
        auto specifiedDataLinkMode = tableConfig.GetDataLinkMode();
        if (!realtimeInfo->adaptsToDataLinkMode(specifiedDataLinkMode)) {
            AUTIL_LOG(ERROR,
                      "load realtime info from %s for %s failed: adapts to DataLinkMode[%s] failed",
                      indexRoot.c_str(),
                      autil::legacy::FastToJsonString(pid).c_str(),
                      specifiedDataLinkMode.c_str());
            return false;
        }
        AUTIL_LOG(DEBUG, "construct realtime info from index root: %s", indexRoot.c_str());
    }
    return true;
}

} // namespace suez
