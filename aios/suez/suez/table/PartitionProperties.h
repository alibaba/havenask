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
#pragma once

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/MergeControllerConfig.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/index_partition_options.h"
#include "suez/drc/DrcConfig.h"
#include "suez/sdk/PartitionId.h"
#include "suez/table/TableMeta.h"
#include "suez/table/wal/WALConfig.h"

namespace suez {

typedef build_service::config::MergeControllerConfig MergeControllerConfig;

class TableConfig final : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

    bool loadFromFile(const std::string &path, const std::string &fileName, bool isIndexPartition);

public:
    bool hasRealtime() const { return _realtime; }
    bool disableFollowerBuild() const { return _disableFollowerBuild; }
    bool directWrite() const { return _directWrite; }
    bool asyncDirectWrite() const { return _asyncDirectWrite; }
    const indexlibv2::config::TabletOptions &getTabletOptions() const { return _tabletOptions; }
    const MergeControllerConfig &getMergeControllerConfig() const { return _mergeControllerConfig; }
    bool hasMergeControllerConfig() const { return _hasMergeControllerConfig; }
    DrcConfig &getDrcConfig() { return _drcConfig; }
    const DrcConfig &getDrcConfig() const { return _drcConfig; }
    const WALConfig &getWALConfig() const { return _walConfig; }
    const std::map<std::string, std::string> &getRealtimeInfo() const { return _realtimeInfo; }

private:
    bool _realtime = false;
    bool _disableFollowerBuild = false;
    bool _directWrite = false;
    bool _asyncDirectWrite = false;
    indexlibv2::config::TabletOptions _tabletOptions;
    MergeControllerConfig _mergeControllerConfig;
    bool _hasMergeControllerConfig = false;
    DrcConfig _drcConfig;
    WALConfig _walConfig;
    std::map<std::string, std::string> _realtimeInfo;
    // TODO: cluster_config and build_option_config
};

class PartitionProperties {
public:
    explicit PartitionProperties(const PartitionId &pid);
    ~PartitionProperties();

public:
    bool init(const TargetPartitionMeta &target, const TableConfig &tableConfig);

    bool needReadRemoteIndex() const { return tabletOptions.GetNeedReadRemoteIndex(); }

    const std::string getTableName() const { return _pid.getTableName(); }

private:
    const PartitionId &_pid;

public:
    bool hasRt = false;
    bool disableFollowerBuild = false;
    bool allowFollowerWrite = false;
    bool directWrite = false;
    bool asyncDirectWrite = false;
    RoleType roleType = RT_FOLLOWER;
    IncVersion version = INVALID_VERSION;
    indexlibv2::config::TabletOptions tabletOptions;
    indexlib::config::IndexPartitionOptions indexOptions; // TODO: rm
    std::string primaryIndexRoot;
    std::string secondaryIndexRoot;
    std::string rawIndexRoot;
    std::string schemaRoot;
    std::string localConfigPath;
    std::string remoteConfigPath;
    build_service::workflow::RealtimeInfoWrapper realtimeInfo;
    MergeControllerConfig mergeControllerConfig;
    DrcConfig drcConfig;
    WALConfig walConfig;
};

} // namespace suez
