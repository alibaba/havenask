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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "indexlib/partition/index_application.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "suez/sdk/TableReader.h"

namespace indexlibv2 {
namespace framework {
class ITablet;
}
} // namespace indexlibv2

namespace suez {
namespace turing {
class TableInfo;

typedef std::shared_ptr<TableInfo> TableInfoPtr;
typedef std::map<std::string, int32_t> TableVersionMap;

class MultiTableWrapper {
public:
    MultiTableWrapper() = default;
    ~MultiTableWrapper() = default;

    using TabletMap = std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>>;

private:
    MultiTableWrapper(const MultiTableWrapper &) = delete;
    MultiTableWrapper &operator=(const MultiTableWrapper &) = delete;

public:
    bool init(const suez::MultiTableReader &multiTableReader,
              const std::vector<std::string> &filterTables,
              const indexlib::partition::JoinRelationMap &joinRelationMap,
              const std::string &itemTableName = "");

    std::map<int32_t, indexlib::partition::IndexApplicationPtr> getId2IndexAppMap() const { return _id2IndexAppMap; }

    std::map<int32_t, std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>>> getId2TabletMap() const {
        return _id2TabletMap;
    }

    TableInfoPtr getTableInfoWithRelation() const { return _tableInfoWithRel; }
    const std::map<std::string, TableInfoPtr> &getTableInfoMapWithoutRelation() const {
        return _tableInfoMapWithoutRel;
    }
    int32_t getMaxTablePartCount() const { return _maxPartCount; }

private:
    bool getRequiredTablesReader(const suez::MultiTableReader &multiTableReader,
                                 const std::vector<std::string> &requiredTables,
                                 suez::SingleTableReaderMapMap &singleTableReaderMapMap);
    int32_t calMaxPartCount(const suez::SingleTableReaderMapMap &singleTableReaderMapMap,
                            std::string &maxPartTableName);
    bool calcPartPos(const SingleTableReaderMapMap &singleTableReaderMapMap,
                     int32_t maxPartCount,
                     const std::string &maxPartTableName,
                     std::vector<int32_t> &partPos);
    bool findPartPos(const suez::PartitionId &partitionId, int32_t maxPartCount, size_t &partId);

    bool createTabletMap(const suez::SingleTableReaderMapMap &singleTableReaderMapMap,
                         int32_t maxPartCount,
                         const std::vector<int32_t> &partPos);
    bool createSingleTabletMap(const SingleTableReaderMapMap &singleTableReaderMapMap);
    bool createMultiTabletMap(const SingleTableReaderMapMap &singleTableReaderMapMap,
                              const std::vector<int32_t> &partPos);

    bool createIndexApplications(const suez::SingleTableReaderMapMap &singleTableReaderMapMap,
                                 const indexlib::partition::JoinRelationMap &joinRelationMap,
                                 int32_t maxPartCount,
                                 const std::vector<int32_t> &partPos);
    bool createSingleIndexApplication(const suez::SingleTableReaderMapMap &singleTableReaderMapMap,
                                      const indexlib::partition::JoinRelationMap &joinRelationMap);
    bool createMultiIndexApplication(const suez::SingleTableReaderMapMap &singleTableReaderMapMap,
                                     const indexlib::partition::JoinRelationMap &joinRelationMap,
                                     const int32_t maxPartCount);

    indexlib::partition::IndexApplicationPtr
    createIndexApplication(const suez::IndexPartitionMap &indexPartitionMap,
                           const indexlib::partition::JoinRelationMap &joinRelationMap,
                           const TabletMap &tabletMap);

    void genTableInfoWithRel(const std::string &itemTableName);
    void genTableInfoMap(const TableVersionMap &tableVersionMap,
                         const suez::IndexPartitionMap &indexPartitions,
                         const TabletMap &tabletMap);
    bool checkPartInSameFullVersion(const SingleTableReaderMap &singleTableReaderMap);

private:
    int32_t _maxPartCount = -1;
    TableInfoPtr _tableInfoWithRel;
    std::map<std::string, TableInfoPtr> _tableInfoMapWithoutRel;
    std::map<int32_t, indexlib::partition::IndexApplicationPtr> _id2IndexAppMap;
    std::map<int32_t, std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>>> _id2TabletMap;
};
} // namespace turing
} // namespace suez
