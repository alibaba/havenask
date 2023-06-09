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
#include "resource_reader/ResourceReader.h"
#include "suez/sdk/PathDefine.h"
#include "suez/table/TablePathDefine.h"

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
}

bool TableConfig::loadFromFile(const std::string &path, const std::string &fileName, bool isIndexPartition) {
    resource_reader::ResourceReader resourceReader(path);

    std::string configContent;
    if (!resourceReader.getConfigContent(fileName, configContent)) {
        AUTIL_LOG(ERROR, "read config from %s / %s failed", path.c_str(), fileName.c_str());
        return false;
    }

    try {
        autil::legacy::FromJsonString(*this, configContent);
        autil::legacy::FromJsonString(_tabletOptions, configContent);
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
    if (!this->tabletOptions.IsRawJsonEmpty() && !this->tabletOptions.GetFromRawJson("", &(this->indexOptions))) {
        AUTIL_LOG(ERROR, "rejsonize to IndexPartitionOptions for %s failed", autil::legacy::ToJsonString(_pid).c_str());
        return false;
    }

    this->primaryIndexRoot = TablePathDefine::constructIndexPath(PathDefine::getLocalIndexRoot(), _pid);
    this->secondaryIndexRoot = TablePathDefine::constructIndexPath(target.getIndexRoot(), _pid);
    this->rawIndexRoot = TablePathDefine::constructIndexPath(target.getRawIndexRoot(), _pid);
    this->schemaRoot = tabletOptions.GetNeedReadRemoteIndex() ? this->secondaryIndexRoot : this->primaryIndexRoot;

    if (this->hasRt || this->directWrite) {
        const auto &realtimeInfoFromConfig = tableConfig.getRealtimeInfo();
        if (!realtimeInfoFromConfig.empty()) {
            // TODO: validate
            realtimeInfo = build_service::workflow::RealtimeInfoWrapper(realtimeInfoFromConfig);
            AUTIL_LOG(INFO, "construct realtime info from table config");
        } else {
            if (!build_service::workflow::RealtimeBuilder::loadRealtimeInfo(schemaRoot, &realtimeInfo)) {
                AUTIL_LOG(ERROR,
                          "load realtime info from %s for %s failed",
                          schemaRoot.c_str(),
                          autil::legacy::ToJsonString(_pid).c_str());
                return false;
            }
            if (!tableConfig.hasMergeControllerConfig() && !realtimeInfo.getBsServerAddress().empty()) {
                // when no merge controller config && has remote bs service, use remote mode
                this->mergeControllerConfig.mode = "remote";
            }
            AUTIL_LOG(INFO, "construct realtime info from index root: %s", schemaRoot.c_str());
        }
    }
    return true;
}

} // namespace suez
