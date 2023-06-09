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
#include "suez/sdk/TableReader.h"

#include <algorithm>
#include <assert.h>
#include <iosfwd>
#include <utility>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/framework/ITablet.h"
#include "suez/sdk/SuezIndexPartitionData.h"
#include "suez/sdk/SuezRawFilePartitionData.h"
#include "suez/sdk/SuezTabletPartitionData.h"

using namespace std;
using namespace autil::legacy;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SingleTableReader);

SingleTableReader::SingleTableReader(const SuezPartitionDataPtr &partitionData) : _partitionData(partitionData) {}

SingleTableReader::~SingleTableReader() {}

bool SingleTableReader::hasRt() const {
    if (SPT_INDEXLIB == _partitionData->type()) {
        auto indexPartitionData = static_pointer_cast<SuezIndexPartitionData>(_partitionData);
        return indexPartitionData->hasRt();
    } else if (SPT_TABLET == _partitionData->type()) {
        auto tabletPartitionData = static_pointer_cast<SuezTabletPartitionData>(_partitionData);
        return tabletPartitionData->hasRt();
    } else {
        return false;
    }
}

indexlib::partition::IndexPartitionPtr SingleTableReader::getIndexPartition() const {
    if (SPT_INDEXLIB == _partitionData->type()) {
        auto indexPartitionData = static_pointer_cast<SuezIndexPartitionData>(_partitionData);
        return indexPartitionData->getIndexPartition();
    } else {
        AUTIL_LOG(
            DEBUG, "table[%s] is not indexlib table", ToJsonString(_partitionData->getPartitionId(), true).c_str());
        return nullptr;
    }
}

std::shared_ptr<indexlibv2::framework::ITablet> SingleTableReader::getTablet() const {
    if (SPT_TABLET == _partitionData->type()) {
        auto tabletPartitionData = static_pointer_cast<SuezTabletPartitionData>(_partitionData);
        return tabletPartitionData->getTablet();
    } else {
        AUTIL_LOG(
            DEBUG, "table [%s] is not tablet table", ToJsonString(_partitionData->getPartitionId(), true).c_str());
        return nullptr;
    }
}

string SingleTableReader::getFilePath() const {
    if (SPT_RAWFILE == _partitionData->type()) {
        auto rawFilePartitionData = static_pointer_cast<SuezRawFilePartitionData>(_partitionData);
        return rawFilePartitionData->getFilePath();
    } else {
        AUTIL_LOG(
            DEBUG, "table[%s] is not rawfile table", ToJsonString(_partitionData->getPartitionId(), true).c_str());
        return string();
    }
}

int32_t SingleTableReader::getTotalPartitionCount() const {
    if (SPT_INDEXLIB == _partitionData->type()) {
        auto indexPartitionData = static_pointer_cast<SuezIndexPartitionData>(_partitionData);
        return indexPartitionData->getTotalPartitionCount();
    } else if (SPT_TABLET == _partitionData->type()) {
        auto tabletPartitionData = static_pointer_cast<SuezTabletPartitionData>(_partitionData);
        return tabletPartitionData->getTotalPartitionCount();
    } else {
        return 1;
    }
}

// return false if both is null
bool SingleTableReader::operator==(const SingleTableReader &other) const {
    if (this == &other) {
        return true;
    }
    if (!_partitionData || !other._partitionData) {
        return false;
    }
    return *_partitionData == *other._partitionData;
}

bool SingleTableReader::operator!=(const SingleTableReader &other) const { return !(*this == other); }

MultiTableReader::MultiTableReader() {}

MultiTableReader::~MultiTableReader() {}

void MultiTableReader::addTableReader(const PartitionId &pid, const SingleTableReaderPtr &singleTableReader) {
    _tableReaders[pid] = singleTableReader;
    _namedTableReaders[pid.getTableName()][pid] = singleTableReader;
}

bool MultiTableReader::hasTableReader(const PartitionId &pid) const {
    return _tableReaders.find(pid) != _tableReaders.end();
}

SingleTableReaderPtr MultiTableReader::getTableReader(const PartitionId &pid) const {
    auto it = _tableReaders.find(pid);
    if (_tableReaders.end() == it) {
        AUTIL_LOG(ERROR, "table reader for [%s] is not found", ToJsonString(pid, true).c_str());
        return nullptr;
    }
    return it->second;
}

SingleTableReaderMap MultiTableReader::getTableReaders(const string &tableName) const {
    auto it = _namedTableReaders.find(tableName);
    if (_namedTableReaders.end() == it) {
        AUTIL_LOG(ERROR, "table reader map for [%s] is not found", tableName.c_str());
        return SingleTableReaderMap();
    }
    return it->second;
}

SingleTableReaderPtr MultiTableReader::getTableReader(const std::string &tableName, uint32_t hashId) const {
    for (const auto &[pid, reader] : _tableReaders) {
        if (pid.covers(hashId) && pid.getTableName() == tableName) {
            return reader;
        }
    }
    return nullptr;
}

SingleTableReaderPtr
MultiTableReader::getTableReaderByRange(RangeType from, RangeType to, const string &tableName) const {
    for (auto &it : _tableReaders) {
        if (it.first.from == from && it.first.to == to) {
            if (it.first.tableId.tableName == tableName) {
                return it.second;
            }
        }
    }
    AUTIL_LOG(ERROR, "table reader for partition[%d_%d] tableName[%s] not found", from, to, tableName.c_str());
    return nullptr;
}

std::shared_ptr<indexlibv2::framework::ITablet> MultiTableReader::getTabletByIdx(const std::string &tableName,
                                                                                 int32_t index) const {
    auto reader = getTableReaderByIdx(tableName, index);
    if (reader) {
        return reader->getTablet();
    }
    AUTIL_LOG(ERROR, "table reader not found");
    return nullptr;
}

SingleTableReaderPtr MultiTableReader::getTableReaderByIdx(const std::string &tableName, int32_t index) const {
    auto tableReaders = getTableReaders(tableName);
    if (tableReaders.empty()) {
        AUTIL_LOG(ERROR, "table %s not found", tableName.c_str());
        return nullptr;
    }
    for (const auto &iter : tableReaders) {
        if (iter.first.index == index) {
            return iter.second;
        }
    }
    AUTIL_LOG(ERROR, "table found, index [%d] not found", index);
    return nullptr;
}

TablePartitionsMap MultiTableReader::getTablePartitionsMap() const {
    TablePartitionsMap result;
    for (auto &it : _namedTableReaders) {
        const string &name = it.first;
        const SingleTableReaderMap &partitions = it.second;
        TableReader &tableReader = result[name];
        for (auto const &pit : partitions) {
            tableReader[pit.first] = pit.second->getIndexPartition();
        }
    }
    return result;
}

bool MultiTableReader::hasRt(const string &tableName) const {
    auto it = _namedTableReaders.find(tableName);
    if (it == _namedTableReaders.end()) {
        return false;
    }
    const auto &tablePartitions = it->second;
    bool hasRt = false;
    for (const auto &pit : tablePartitions) {
        hasRt = pit.second->hasRt();
        break;
    }
    return hasRt;
}

TableReader MultiTableReader::getIndexPartitions(const string &tableName) const {
    auto singleTableReaders = getTableReaders(tableName);
    TableReader result;
    for (auto it : singleTableReaders) {
        auto partition = it.second->getIndexPartition();
        if (partition != nullptr) {
            result[it.first] = partition;
        }
    }
    return result;
}

std::vector<std::shared_ptr<indexlibv2::framework::ITablet>>
MultiTableReader::getTablets(const string &tableName) const {
    auto singleTableReaderMap = getTableReaders(tableName);
    std::vector<std::shared_ptr<indexlibv2::framework::ITablet>> result;
    for (const auto &[_, singleTableReader] : singleTableReaderMap) {
        auto tablet = singleTableReader->getTablet();
        if (tablet != nullptr) {
            result.push_back(tablet);
        }
    }
    return result;
}

void MultiTableReader::erase(const PartitionId &pid) {
    _tableReaders.erase(pid);
    string tableName = pid.getTableName();
    if (_namedTableReaders.find(tableName) != _namedTableReaders.end()) {
        _namedTableReaders[tableName].erase(pid);
        if (_namedTableReaders[tableName].empty()) {
            _namedTableReaders.erase(tableName);
        }
    }
}
void MultiTableReader::clear() {
    _tableReaders.clear();
    _namedTableReaders.clear();
}

TableReader MultiTableReader::getIndexlibTableReaders() const {
    TableReader ret;
    for (const auto &[pid, singleTableReader] : _tableReaders) {
        const auto &indexPartition = singleTableReader->getIndexPartition();
        if (!indexPartition) {
            continue;
        }
        ret[pid] = indexPartition;
    }
    return ret;
}

TabletReader MultiTableReader::getIndexlibTabletReaders() const {
    TabletReader ret;
    for (const auto &[pid, singleTableReader] : _tableReaders) {
        const auto &tablet = singleTableReader->getTablet();
        if (!tablet) {
            continue;
        }
        ret[pid] = tablet;
    }
    return ret;
}

vector<string> MultiTableReader::getTableNames() const {
    vector<string> tableNames;
    SingleTableReaderMap::const_iterator it = _tableReaders.begin();
    for (; it != _tableReaders.end(); it++) {
        tableNames.push_back(ToJsonString(it->first, true).c_str());
    }
    return tableNames;
}

set<string> MultiTableReader::getTableNameOnlySet() const {
    set<string> tableNames;
    map<string, SingleTableReaderMap>::const_iterator it = _namedTableReaders.begin();
    for (; it != _namedTableReaders.end(); it++) {
        tableNames.insert(it->first);
    }
    return tableNames;
}

vector<PartitionId> MultiTableReader::getPartitionIds(const string &tableName) const {
    vector<PartitionId> pids;
    auto tableReaders = getTableReaders(tableName);
    for (const auto &it : tableReaders) {
        pids.push_back(it.first);
    }
    return pids;
}

bool operator==(const MultiTableReader &left, const MultiTableReader &right) {
    if (left._tableReaders.size() != right._tableReaders.size()) {
        AUTIL_LOG(INFO, "table count changed, [%zd] != [%zd].", left._tableReaders.size(), right._tableReaders.size());
        return false;
    }
    SingleTableReaderMap::const_iterator it1 = left._tableReaders.begin();
    SingleTableReaderMap::const_iterator it2 = right._tableReaders.begin();
    for (; it1 != left._tableReaders.end() && it2 != right._tableReaders.end(); ++it1, ++it2) {
        if (it1->first != it2->first) {
            AUTIL_LOG(INFO,
                      "table changed, left tableNames:[%s], right tableNames:[%s].",
                      ToJsonString(left.getTableNames(), true).c_str(),
                      ToJsonString(right.getTableNames(), true).c_str());
            return false;
        }
        if (*(it1->second) != *(it2->second)) {
            AUTIL_LOG(INFO, "[%s] table changed", ToJsonString(it1->first, true).c_str());
            return false;
        }
    }
    return true;
}

bool operator!=(const MultiTableReader &left, const MultiTableReader &right) { return !(left == right); }

} // namespace suez
