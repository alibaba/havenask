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
#include "suez/table/TabletAdapter.h"

#include "autil/Log.h"
#include "build_service/common/ConfigDownloader.h"
#include "build_service/config/ResourceReader.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/Version.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/MultiCounter.h"
#include "suez/common/TablePathDefine.h"
#include "suez/sdk/SuezTabletPartitionData.h"
#include "suez/sdk/TableWriter.h"
#include "suez/table/AccessCounterLog.h"
#include "suez/table/DirectBuilder.h"
#include "suez/table/DummyTableBuilder.h"
#include "suez/table/PartitionProperties.h"
#include "suez/table/RealtimeBuilderV2Wrapper.h"

using namespace std;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TabletAdapter);

TabletAdapter::TabletAdapter(const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
                             std::string localRoot,
                             std::string remoteRoot,
                             const std::shared_ptr<indexlibv2::config::TabletSchema> &schema,
                             RoleType roleType)
    : _tablet(tablet), _indexRoot(std::move(localRoot), std::move(remoteRoot)), _schema(schema), _roleType(roleType) {}

TabletAdapter::~TabletAdapter() {
    if (_tablet) {
        if (_tablet.use_count() != 1) {
            AUTIL_LOG(WARN,
                      "close tablet name [%s] pointer [%p] when use count = [%ld]",
                      _schema->GetTableName().c_str(),
                      _tablet.get(),
                      _tablet.use_count());
        }
        _tablet->Close();
    }
}

static TableStatus convertTableStatus(const indexlib::Status &status) {
    if (status.IsOK()) {
        return TS_LOADED;
    }
    if (status.IsNoMem()) {
        return TS_ERROR_LACK_MEM;
    } else if (status.IsConfigError()) {
        return TS_ERROR_CONFIG;
    } else {
        return TS_FORCE_RELOAD;
    }
}

TableStatus TabletAdapter::open(const PartitionProperties &properties, const TableVersion &tableVersion) {
    auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>(properties.tabletOptions);
    if (_roleType == RT_LEADER) {
        tabletOptions->SetIsLeader(true);
        tabletOptions->SetFlushRemote(true);
        // TODO: flush local from config
        tabletOptions->SetFlushLocal(false);
    } else {
        tabletOptions->SetIsLeader(false);
        tabletOptions->SetFlushRemote(false);
        tabletOptions->SetFlushLocal(true);
    }

    auto versionId = tableVersion.getVersionId();
    if (versionId < 0) {
        versionId = INVALID_VERSION;
    }
    auto status = _tablet->Open(_indexRoot, _schema, std::move(tabletOptions), versionId);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "open failed, error: %s", status.ToString().c_str());
    }
    return convertTableStatus(status);
}

TableStatus TabletAdapter::reopen(bool force, const TableVersion &tableVersion) {
    indexlibv2::framework::ReopenOptions opts;
    opts.SetForce(force);

    auto s = _tablet->Reopen(opts, tableVersion.getVersionId());
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "reopen failed, error: %s", s.ToString().c_str());
    }
    return convertTableStatus(s);
}

void TabletAdapter::cleanIndexFiles(const std::set<IncVersion> &toKeepVersions) {
    std::vector<indexlibv2::versionid_t> versionVec;
    std::for_each(toKeepVersions.begin(), toKeepVersions.end(), [&versionVec](const auto &version) {
        versionVec.push_back(version);
    });
    auto s = _tablet->CleanIndexFiles(versionVec);
    if (!s.IsOK()) {
        AUTIL_LOG(WARN, "clean index files failed, error: %s", s.ToString().c_str());
    }
}

bool TabletAdapter::cleanUnreferencedIndexFiles(const std::set<std::string> &toKeepFiles) {
    if (_tablet != nullptr) {
        auto status = _tablet->CleanUnreferencedDeployFiles(toKeepFiles);
        if (!status.IsOK()) {
            AUTIL_LOG(WARN, "clean deployed local index files failed, error: %s", status.ToString().c_str());
            return false;
        }
        return true;
    }
    return false;
}

void TabletAdapter::reportAccessCounter(const std::string &name) const {
    AccessCounterLog accessCounterLog;
    if (!_tablet) {
        return;
    }
    const auto *tabletInfos = _tablet->GetTabletInfos();
    if (!tabletInfos) {
        return;
    }
    auto counterMap = tabletInfos->GetCounterMap();

    auto attributeCounter = counterMap->GetMultiCounter("online.access.attribute");
    for (const auto &[attributeName, counter] : attributeCounter->GetCounterMap()) {
        auto accCounter = std::dynamic_pointer_cast<indexlib::util::AccumulativeCounter>(counter);
        if (accCounter) {
            accessCounterLog.reportAttributeCounter(name + "." + attributeName, accCounter->Get());
        }
    }

    auto invertedCounter = counterMap->GetMultiCounter("online.access.inverted_index");
    for (const auto &[invertedName, counter] : invertedCounter->GetCounterMap()) {
        auto accCounter = std::dynamic_pointer_cast<indexlib::util::AccumulativeCounter>(counter);
        if (accCounter) {
            accessCounterLog.reportIndexCounter(name + "." + invertedName, accCounter->Get());
        }
    }
}

std::shared_ptr<indexlibv2::config::ITabletSchema> TabletAdapter::getSchema() const {
    return _tablet->GetTabletSchema();
}

std::pair<TableVersion, SchemaVersion> TabletAdapter::getLoadedVersion() const {
    auto tabletInfos = _tablet->GetTabletInfos();
    if (tabletInfos == nullptr) {
        return {TableVersion(), 0};
    }
    auto loadedVersion = tabletInfos->GetLoadedPublishVersion();
    TableVersion version(loadedVersion.GetVersionId(),
                         indexlibv2::framework::VersionMeta(loadedVersion, /*docCount*/ -1, /*indexSize*/ -1));

    auto schema = _tablet->GetTabletSchema();
    if (schema == nullptr) {
        return {std::move(version), 0};
    }
    return {std::move(version), schema->GetSchemaId()};
}

TableBuilder *TabletAdapter::createBuilder(const build_service::proto::PartitionId &pid,
                                           const build_service::workflow::RealtimeBuilderResource &rtResource,
                                           const PartitionProperties &properties) const {
    const string &configPath = properties.localConfigPath;
    bool directWrite = properties.directWrite;
    if (directWrite) {
        AUTIL_LOG(INFO,
                  "create DirectBuilder for pid [%s], config path [%s]",
                  pid.ShortDebugString().c_str(),
                  configPath.c_str());
        auto localConfigPath = TablePathDefine::constructTempConfigPath(
            pid.clusternames(0), pid.range().from(), pid.range().to(), configPath);
        assert(!localConfigPath.empty());
        auto errorCode = build_service::common::ConfigDownloader::downloadConfig(configPath, localConfigPath);

        if (errorCode == build_service::common::ConfigDownloader::DEC_NORMAL_ERROR ||
            errorCode == build_service::common::ConfigDownloader::DEC_DEST_ERROR) {
            AUTIL_LOG(
                ERROR, "download schema config from [%s] to [%s] failed", configPath.c_str(), localConfigPath.c_str());
            return nullptr;
        }

        return new DirectBuilder(pid, _tablet, rtResource, localConfigPath, properties.walConfig);
    }
    if (_tablet->GetTabletInfos()->GetLoadedPublishVersion().IsSealed()) {
        AUTIL_LOG(INFO, "[%s] is sealed, do not need to start realtime builder", pid.ShortDebugString().c_str());
        return new DummyTableBuilder();
    }
    return new RealtimeBuilderV2Wrapper(pid, _tablet, rtResource, configPath);
}

std::shared_ptr<SuezPartitionData>
TabletAdapter::createSuezPartitionData(const PartitionId &pid, uint64_t totalPartitionCount, bool hasRt) const {
    return std::make_shared<SuezTabletPartitionData>(pid, totalPartitionCount, _tablet, hasRt);
}

std::shared_ptr<indexlibv2::framework::ITablet> TabletAdapter::getTablet() const { return _tablet; }

} // namespace suez
