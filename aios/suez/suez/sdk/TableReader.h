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

#include <map>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "indexlib/partition/index_partition.h"
#include "suez/sdk/SuezPartitionData.h"

namespace indexlibv2 {
namespace framework {
class ITablet;
}
} // namespace indexlibv2

namespace suez {

typedef std::map<PartitionId, std::shared_ptr<indexlibv2::framework::ITablet>> TabletReader;
typedef std::map<PartitionId, indexlib::partition::IndexPartitionPtr> TableReader;
typedef std::map<std::string, indexlib::partition::IndexPartitionPtr> IndexPartitionMap;
typedef std::map<std::string, TableReader> TablePartitionsMap; // tableName --> pid1, pid2

class Table;

typedef std::shared_ptr<Table> TablePtr;

class SingleTableReader {
public:
    SingleTableReader(const SuezPartitionDataPtr &_partitionData);
    virtual ~SingleTableReader();

private:
    SingleTableReader(const SingleTableReader &);
    SingleTableReader &operator=(const SingleTableReader &);

public:
    bool hasRt() const;
    indexlib::partition::IndexPartitionPtr getIndexPartition() const;
    std::shared_ptr<indexlibv2::framework::ITablet> getTablet() const;
    std::string getFilePath() const;
    template <typename T>
    std::shared_ptr<T> get() const {
        assert(false);
    }
    bool operator==(const SingleTableReader &other) const;
    bool operator!=(const SingleTableReader &other) const;
    int32_t getTotalPartitionCount() const;

private:
    SuezPartitionDataPtr _partitionData;
};

template <>
inline std::shared_ptr<indexlibv2::framework::ITablet> SingleTableReader::get() const {
    return this->getTablet();
}

template <>
inline indexlib::partition::IndexPartitionPtr SingleTableReader::get() const {
    return this->getIndexPartition();
}

typedef std::shared_ptr<SingleTableReader> SingleTableReaderPtr;
typedef std::map<PartitionId, SingleTableReaderPtr> SingleTableReaderMap;
typedef std::map<std::string, SingleTableReaderMap> SingleTableReaderMapMap;

class MultiTableReader {
public:
    MultiTableReader();
    virtual ~MultiTableReader();

public:
    void addTableReader(const PartitionId &pid, const SingleTableReaderPtr &singleTableReader);
    bool hasTableReader(const PartitionId &pid) const;
    SingleTableReaderPtr getTableReader(const PartitionId &pid) const;
    SingleTableReaderPtr getTableReader(const std::string &tableName, uint32_t hashId) const;
    SingleTableReaderPtr getTableReaderByRange(RangeType from, RangeType to, const std::string &tableName) const;
    std::shared_ptr<indexlibv2::framework::ITablet> getTabletByIdx(const std::string &tableName, int32_t index) const;
    SingleTableReaderPtr getTableReaderByIdx(const std::string &tableName, int32_t index) const;
    void erase(const PartitionId &pid);
    SingleTableReaderMap getTableReaders(const std::string &tableName) const;
    TableReader getIndexlibTableReaders() const;
    TableReader getIndexPartitions(const std::string &tableName) const;
    TabletReader getIndexlibTabletReaders() const;
    std::vector<std::shared_ptr<indexlibv2::framework::ITablet>> getTablets(const std::string &tableName) const;
    TablePartitionsMap getTablePartitionsMap() const;
    bool hasRt(const std::string &tableName) const;
    void clear();
    std::vector<PartitionId> getPartitionIds(const std::string &tableName) const;

    friend bool operator==(const MultiTableReader &left, const MultiTableReader &right);
    friend bool operator!=(const MultiTableReader &left, const MultiTableReader &right);

    std::vector<std::string> getTableNames() const;
    std::set<std::string> getTableNameOnlySet() const;
    const SingleTableReaderMapMap &getAllTableReaders() const { return _namedTableReaders; }

private:
    SingleTableReaderMap _tableReaders;
    SingleTableReaderMapMap _namedTableReaders;
};

bool operator==(const MultiTableReader &left, const MultiTableReader &right);
bool operator!=(const MultiTableReader &left, const MultiTableReader &right);

} // namespace suez
