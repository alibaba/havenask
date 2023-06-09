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
#include "suez/table/IndexPartitionAdapter.h"

#include <algorithm>

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "suez/sdk/SuezIndexPartitionData.h"
#include "suez/sdk/TableWriter.h"
#include "suez/table/AccessCounterLog.h"
#include "suez/table/PartitionProperties.h"
#include "suez/table/RealtimeBuilderWrapper.h"

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, IndexPartitionAdapter);

IndexPartitionAdapter::IndexPartitionAdapter(const std::string &primaryIndexRoot,
                                             const std::string &secondaryIndexRoot,
                                             const indexlib::partition::IndexPartitionPtr &indexPart)
    : _primaryIndexRoot(primaryIndexRoot), _secondaryIndexRoot(secondaryIndexRoot), _indexPartition(indexPart) {}

IndexPartitionAdapter::~IndexPartitionAdapter() {
    if (_indexPartition) {
        try {
            _indexPartition->Close();
        } catch (const autil::legacy::ExceptionBase &e) {
            AUTIL_LOG(ERROR, "error occurred in OnlinePartition deconstructor[%s]", e.what());
        }
    }
}

static TableStatus convertTableStatus(indexlib::partition::IndexPartition::OpenStatus os) {
    switch (os) {
    case indexlib::partition::IndexPartition::OS_OK:
        return TS_LOADED;
    case indexlib::partition::IndexPartition::OS_LACK_OF_MEMORY:
    case indexlib::partition::IndexPartition::OS_FORCE_REOPEN:
        return TS_ERROR_LACK_MEM;
    case indexlib::partition::IndexPartition::OS_INCONSISTENT_SCHEMA:
    case indexlib::partition::IndexPartition::OS_FILEIO_EXCEPTION:
    case indexlib::partition::IndexPartition::OS_INDEXLIB_EXCEPTION:
    case indexlib::partition::IndexPartition::OS_UNKNOWN_EXCEPTION:
        return TS_FORCE_RELOAD;
    default:
        // NOTE: indexlib might return OS_FAIL on lacking memory.
        return TS_ERROR_UNKNOWN;
    }
}

TableStatus IndexPartitionAdapter::open(const PartitionProperties &properties, const TableVersion &tableVersion) {
    auto openStatus = _indexPartition->Open(_primaryIndexRoot,
                                            _secondaryIndexRoot,
                                            /*schema*/ nullptr,
                                            properties.indexOptions,
                                            tableVersion.getVersionId());
    return convertTableStatus(openStatus);
}

TableStatus IndexPartitionAdapter::reopen(bool force, const TableVersion &tableVersion) {
    auto openStatus = _indexPartition->ReOpen(force, tableVersion.getVersionId());
    return convertTableStatus(openStatus);
}

void IndexPartitionAdapter::cleanIndexFiles(const std::set<IncVersion> &keepSet) {
    if (_indexPartition.get() != nullptr) {
        std::vector<IncVersion> toKeep;
        std::for_each(keepSet.begin(), keepSet.end(), [&toKeep](const auto &version) { toKeep.push_back(version); });
        _indexPartition->CleanIndexFiles(toKeep);
    }
}

bool IndexPartitionAdapter::cleanUnreferencedIndexFiles(const std::set<std::string> &toKeepFiles) {
    if (_indexPartition.get() != nullptr) {
        return _indexPartition->CleanUnreferencedIndexFiles(toKeepFiles);
    }
    return false;
}

void IndexPartitionAdapter::reportAccessCounter(const std::string &name) const {
    AccessCounterLog accessCounterLog;
    if (_indexPartition.get() == nullptr) {
        return;
    }
    auto reader = _indexPartition->GetReader();
    if (reader.get() == nullptr) {
        return;
    }
    // report index counters
    const auto &indexCounters = reader->GetIndexAccessCounters();
    for (auto it = indexCounters.begin(); it != indexCounters.end(); it++) {
        accessCounterLog.reportIndexCounter(name + "." + it->first, it->second->Get());
    }
    // report attribute counters
    const auto &attributeCounters = reader->GetAttributeAccessCounters();
    for (auto it = attributeCounters.begin(); it != attributeCounters.end(); it++) {
        accessCounterLog.reportAttributeCounter(name + "." + it->first, it->second->Get());
    }
}

std::shared_ptr<indexlibv2::config::ITabletSchema> IndexPartitionAdapter::getSchema() const {
    if (_indexPartition.get() == nullptr) {
        return nullptr;
    }
    auto reader = _indexPartition->GetReader();
    if (reader.get() == nullptr) {
        return nullptr;
    }
    auto schema = reader->GetSchema();
    if (!schema) {
        return nullptr;
    }
    return std::make_shared<indexlib::config::LegacySchemaAdapter>(schema);
}

std::pair<TableVersion, SchemaVersion> IndexPartitionAdapter::getLoadedVersion() const {
    if (_indexPartition.get() == nullptr) {
        return {TableVersion(), 0};
    }
    auto reader = _indexPartition->GetReader();
    if (reader.get() == nullptr) {
        return {TableVersion(), 0};
    }
    const auto &version = reader->GetVersion();
    return std::make_pair(TableVersion(version.GetVersionId(), version.ToVersionMeta()), version.GetSchemaVersionId());
}

TableBuilder *IndexPartitionAdapter::createBuilder(const build_service::proto::PartitionId &pid,
                                                   const build_service::workflow::RealtimeBuilderResource &rtResource,
                                                   const PartitionProperties &properties) const {
    const std::string &configPath = properties.localConfigPath;
    bool directWrite = properties.directWrite;
    if (directWrite) {
        AUTIL_LOG(ERROR, "%s: do not support direct write in old index partition", pid.ShortDebugString().c_str());
        return nullptr;
    }
    return new RealtimeBuilderWrapper(pid, _indexPartition, rtResource, configPath);
}

std::shared_ptr<SuezPartitionData>
IndexPartitionAdapter::createSuezPartitionData(const PartitionId &pid, uint64_t totalPartitionCount, bool hasRt) const {
    return std::make_shared<SuezIndexPartitionData>(pid, totalPartitionCount, _indexPartition, hasRt);
}

} // namespace suez
