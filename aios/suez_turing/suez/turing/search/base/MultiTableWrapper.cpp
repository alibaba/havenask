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
#include "suez/turing/search/base/MultiTableWrapper.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "suez/sdk/TableReader.h"
#include "suez/turing/expression/util/TableInfoConfigurator.h"

using namespace autil;
using namespace std;
using namespace suez;
using namespace indexlib::partition;

namespace suez {
namespace turing {
AUTIL_DECLARE_AND_SETUP_LOGGER(turing, MultiTableWrapper);

bool MultiTableWrapper::init(const suez::MultiTableReader &multiTableReader,
                             const vector<string> &requiredTables,
                             const JoinRelationMap &joinRelationMap,
                             const std::string &itemTableName) {
    SingleTableReaderMapMap singleTableReaderMapMap;
    bool ret = getRequiredTablesReader(multiTableReader, requiredTables, singleTableReaderMapMap);
    if (!ret) {
        return false;
    }
    if (singleTableReaderMapMap.empty()) {
        AUTIL_LOG(INFO, "no index partition found.");
        return true;
    }

    std::string maxPartTableName;
    _maxPartCount = calMaxPartCount(singleTableReaderMapMap, maxPartTableName);

    vector<int32_t> partPos;
    if (!calcPartPos(singleTableReaderMapMap, _maxPartCount, maxPartTableName, partPos)) {
        return false;
    }

    if (!createIndexApplications(singleTableReaderMapMap, joinRelationMap, _maxPartCount, partPos)) {
        AUTIL_LOG(ERROR, "create index application failed.");
        return false;
    }

    if (!createTabletMap(singleTableReaderMapMap, _maxPartCount, partPos)) {
        AUTIL_LOG(ERROR, "create tablet map failed.");
        return false;
    }

    genTableInfoWithRel(itemTableName);
    string partIds;
    for (const auto &iter : _id2IndexAppMap) {
        partIds += StringUtil::toString(iter.first) + ", ";
    }
    AUTIL_LOG(INFO, "parition count [%lu], ids:[%s]", _id2IndexAppMap.size(), partIds.c_str());
    return true;
}

template <typename T>
bool createSingleMap(const SingleTableReaderMapMap &singleTableReaderMapMap,
                     std::map<std::string, std::shared_ptr<T>> &result,
                     TableVersionMap &tableVersionMap) {
    for (const auto &iter : singleTableReaderMapMap) {
        const string &tableName = iter.first;
        const SingleTableReaderMap &singleTableReaderMap = iter.second;
        if (singleTableReaderMap.size() != 1) {
            AUTIL_LOG(ERROR, "multiple partition for table %s is not supported", tableName.c_str());
            return false;
        }
        auto tablet = singleTableReaderMap.begin()->second->get<T>();
        if (tablet != nullptr) {
            result[tableName] = tablet;
            tableVersionMap[tableName] = singleTableReaderMap.begin()->first.getFullVersion();
        }
    }
    return true;
}

bool MultiTableWrapper::createTabletMap(const SingleTableReaderMapMap &singleTableReaderMapMap,
                                        int32_t maxPartCount,
                                        const vector<int32_t> &partPos) {
    if (maxPartCount == -1) {
        return createSingleTabletMap(singleTableReaderMapMap);
    } else {
        return createMultiTabletMap(singleTableReaderMapMap, partPos);
    }
}

bool MultiTableWrapper::createSingleTabletMap(const SingleTableReaderMapMap &singleTableReaderMapMap) {
    std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>> tabletMap;
    TableVersionMap tableVersionMap;
    if (!createSingleMap<indexlibv2::framework::ITablet>(singleTableReaderMapMap, tabletMap, tableVersionMap)) {
        return false;
    }
    _id2TabletMap[-1] = tabletMap;
    return true;
}

template <typename T>
void createMultiMap(const SingleTableReaderMapMap &singleTableReaderMapMap,
                    std::map<std::string, std::shared_ptr<T>> &result,
                    TableVersionMap &tableVersionMap,
                    size_t idx) {
    for (const auto &iter : singleTableReaderMapMap) {
        const string &tableName = iter.first;
        const SingleTableReaderMap &singleTableReaderMap = iter.second;
        if (singleTableReaderMap.size() == 0) {
            AUTIL_LOG(WARN, "table [%s] partition size is 0.", tableName.c_str());
            continue;
        }
        for (const auto &entry : singleTableReaderMap) {
            if (entry.first.index == idx) {
                auto data = entry.second->get<T>();
                if (data != nullptr) {
                    result[tableName] = data;
                    tableVersionMap[tableName] = entry.first.getFullVersion();
                }
                break;
            }
        }
        /*
        if (singleTableReaderMap.size() > idx) {
            auto readerIter = singleTableReaderMap.begin();
            std::advance(readerIter, idx);
            auto data = readerIter->second->get<T>();
            if (data != nullptr) {
                result[tableName] = data;
                tableVersionMap[tableName] = readerIter->first.getFullVersion();
            }
        } else {
            int32_t curTablePartCnt = singleTableReaderMap.begin()->second->getTotalPartitionCount();
            if (curTablePartCnt <= 1) { // single partition
                auto data = singleTableReaderMap.begin()->second->get<T>();
                if (data != nullptr) {
                    result[tableName] = data;
                    tableVersionMap[tableName] = singleTableReaderMap.begin()->first.getFullVersion();
                }
            }
        }
        */
    }
}

bool MultiTableWrapper::createMultiTabletMap(const SingleTableReaderMapMap &singleTableReaderMapMap,
                                             const vector<int32_t> &partPos) {
    for (size_t i = 0; i < partPos.size(); i++) {
        std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>> tabletMap;
        TableVersionMap tableVersionMap;
        createMultiMap<indexlibv2::framework::ITablet>(singleTableReaderMapMap, tabletMap, tableVersionMap, i);
        _id2TabletMap[partPos[i]] = tabletMap;
    }
    return true;
}

bool MultiTableWrapper::createIndexApplications(const SingleTableReaderMapMap &singleTableReaderMapMap,
                                                const JoinRelationMap &joinRelationMap,
                                                int32_t maxPartCount,
                                                const vector<int32_t> &partPos) {
    if (maxPartCount == -1) {
        // compatiable old, to create single indexApplications
        return createSingleIndexApplication(singleTableReaderMapMap, joinRelationMap);
    } else {
        return createMultiIndexApplication(singleTableReaderMapMap, joinRelationMap, maxPartCount);
    }
}

bool MultiTableWrapper::createSingleIndexApplication(const SingleTableReaderMapMap &singleTableReaderMapMap,
                                                     const JoinRelationMap &joinRelationMap) {
    suez::IndexPartitionMap indexPartitions;
    TabletMap tablets;
    TableVersionMap tableVersionMap;
    if (!createSingleMap<indexlib::partition::IndexPartition>(
            singleTableReaderMapMap, indexPartitions, tableVersionMap)) {
        return false;
    }
    if (!createSingleMap<indexlibv2::framework::ITablet>(singleTableReaderMapMap, tablets, tableVersionMap)) {
        return false;
    }

    auto indexApp = createIndexApplication(indexPartitions, joinRelationMap, tablets);
    if (!indexApp) {
        return false;
    }
    genTableInfoMap(tableVersionMap, indexPartitions, tablets);
    _id2IndexAppMap[-1] = indexApp;
    return true;
}

bool MultiTableWrapper::createMultiIndexApplication(const SingleTableReaderMapMap &singleTableReaderMapMap,
                                                    const JoinRelationMap &joinRelationMap,
                                                    const int32_t maxPartCount) {
    for (int32_t i = 0; i < maxPartCount; i++) {
        suez::IndexPartitionMap indexPartitions;
        TabletMap tablets;
        TableVersionMap tableVersionMap;
        createMultiMap<indexlib::partition::IndexPartition>(
            singleTableReaderMapMap, indexPartitions, tableVersionMap, i);
        // compatitable with tablet framework
        createMultiMap<indexlibv2::framework::ITablet>(singleTableReaderMapMap, tablets, tableVersionMap, i);
        if (indexPartitions.size() == 0 && tablets.size() == 0) {
            continue;
        }
        auto indexApp = createIndexApplication(indexPartitions, joinRelationMap, tablets);
        if (!indexApp) {
            AUTIL_LOG(ERROR, "create index application failed, indexPartitions size [%lu].", indexPartitions.size());
            return false;
        }
        genTableInfoMap(tableVersionMap, indexPartitions, tablets);
        _id2IndexAppMap[i] = indexApp;
    }
    return true;
}

IndexApplicationPtr MultiTableWrapper::createIndexApplication(const IndexPartitionMap &indexPartitionMap,
                                                              const JoinRelationMap &joinRelationMap,
                                                              const TabletMap &tabletMap) {
    IndexApplicationPtr indexApplication(new IndexApplication());

    if (!indexApplication->Init(indexPartitionMap, joinRelationMap)) {
        AUTIL_LOG(ERROR, "create IndexApplication failed");
        return nullptr;
    }
    if (tabletMap.size() > 0) {
        if (!indexApplication->InitByTablet(tabletMap)) {
            AUTIL_LOG(ERROR, "create tablet IndexApplication failed");
            indexApplication.reset();
        }
    }
    return indexApplication;
}

bool MultiTableWrapper::getRequiredTablesReader(const MultiTableReader &multiTableReader,
                                                const vector<string> &requiredTables,
                                                SingleTableReaderMapMap &singleTableReaderMapMap) {
    auto const &allTableReader = multiTableReader.getAllTableReaders();
    set<string> tableSet;
    for (const auto &iter : allTableReader) {
        const string &tableName = iter.first;
        const SingleTableReaderMap &singleTableReaderMap = iter.second;
        if (requiredTables.empty() ||
            std::find(requiredTables.begin(), requiredTables.end(), tableName) != requiredTables.end()) {
            if (singleTableReaderMap.size() == 0) {
                AUTIL_LOG(WARN, "table name [%s] reader is empty.", tableName.c_str());
                continue;
            }
            tableSet.insert(tableName);
            if (!checkPartInSameFullVersion(singleTableReaderMap)) {
                AUTIL_LOG(WARN, "table name [%s] has multi full version, not support.", tableName.c_str());
                return false;
            }
            if (singleTableReaderMap.begin()->second->getIndexPartition() == nullptr &&
                singleTableReaderMap.begin()->second->getTablet() == nullptr) {
                AUTIL_LOG(INFO, "[%s] is not suez indexlib partition, ignore!", tableName.c_str());
                continue;
            }
            singleTableReaderMapMap[tableName] = singleTableReaderMap;
        }
    }
    for (const auto &requiredTableName : requiredTables) {
        if (tableSet.count(requiredTableName) == 0) {
            AUTIL_LOG(ERROR, "table [%s] not found", requiredTableName.c_str());
            return false;
        }
    }
    return true;
}

int32_t MultiTableWrapper::calMaxPartCount(const SingleTableReaderMapMap &singleTableReaderMapMap,
                                           string &maxPartTableName) {
    int32_t maxPart = -1;
    for (const auto &iter : singleTableReaderMapMap) {
        const auto &tableName = iter.first;
        const auto &singleTableReaderMap = iter.second;
        for (const auto &singleTableReader : singleTableReaderMap) {
            int32_t partCnt = singleTableReader.second->getTotalPartitionCount();
            if (partCnt > maxPart) {
                maxPart = partCnt;
                maxPartTableName = tableName;
            }
        }
    }
    return maxPart;
}

bool MultiTableWrapper::calcPartPos(const SingleTableReaderMapMap &singleTableReaderMapMap,
                                    int32_t maxPartCount,
                                    const string &maxPartTableName,
                                    vector<int32_t> &partPos) {
    if (maxPartCount == -1) {
        return true;
    }
    const auto &maxPartIter = singleTableReaderMapMap.find(maxPartTableName);
    if (maxPartIter == singleTableReaderMapMap.end()) {
        AUTIL_LOG(WARN, "max partition table name [%s] not found.", maxPartTableName.c_str());
        return false;
    }
    const auto &maxPartMapIter = maxPartIter->second;
    auto iter = maxPartMapIter.begin();
    while (iter != maxPartMapIter.end()) {
        const PartitionId &partitionId = iter->first;
        size_t partId;
        if (!findPartPos(partitionId, maxPartCount, partId)) {
            AUTIL_LOG(ERROR, "partition range [%d %d] not fount", partitionId.from, partitionId.to);
            return false;
        }
        partPos.push_back(partId);
        iter++;
    }

    return true;
}

bool MultiTableWrapper::findPartPos(const PartitionId &partitionId, int32_t maxPartCount, size_t &partId) {
    partId = 0;
    const RangeVec &rangeVec = RangeUtil::splitRange(0, RangeUtil::MAX_PARTITION_RANGE, (uint32_t)maxPartCount);
    for (; partId < rangeVec.size(); partId++) {
        if (partitionId.from == rangeVec[partId].first && partitionId.to == rangeVec[partId].second) {
            return true;
        }
    }
    return false;
}

void MultiTableWrapper::genTableInfoWithRel(const std::string &itemTableName) {
    if (!_id2IndexAppMap.empty()) {
        _tableInfoWithRel = TableInfoConfigurator::createFromIndexApp(itemTableName, _id2IndexAppMap.begin()->second);
    }
}

void MultiTableWrapper::genTableInfoMap(const TableVersionMap &tableVersionMap,
                                        const suez::IndexPartitionMap &indexPartitions,
                                        const TabletMap &tabletMap) {

    for (const auto &partitionPair : indexPartitions) {
        if (_tableInfoMapWithoutRel.find(partitionPair.first) != _tableInfoMapWithoutRel.end()) {
            continue;
        }
        const auto &partition = partitionPair.second;
        int32_t version = -1;
        auto it = tableVersionMap.find(partitionPair.first);
        if (it != tableVersionMap.end()) {
            version = it->second;
        }
        _tableInfoMapWithoutRel[partitionPair.first] =
            TableInfoConfigurator::createFromSchema(partition->GetSchema(), version);
    }

    for (const auto &tabletPair : tabletMap) {
        if (_tableInfoMapWithoutRel.find(tabletPair.first) != _tableInfoMapWithoutRel.end()) {
            continue;
        }
        const auto &tablet = tabletPair.second;
        int32_t version = -1;
        auto it = tableVersionMap.find(tabletPair.first);
        if (it != tableVersionMap.end()) {
            version = it->second;
        }
        _tableInfoMapWithoutRel[tabletPair.first] =
            TableInfoConfigurator::createFromSchema(tablet->GetTabletSchema(), version);
    }
}

bool MultiTableWrapper::checkPartInSameFullVersion(const SingleTableReaderMap &singleTableReaderMap) {
    FullVersion version = -1;
    for (auto tableReader : singleTableReaderMap) {
        if (version == -1) {
            version = tableReader.first.tableId.fullVersion;
        }
        if (version != tableReader.first.tableId.fullVersion) {
            return false;
        }
    }
    return true;
}

} // namespace turing

} // namespace suez
