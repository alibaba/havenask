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

#include "autil/legacy/jsonizable.h"

namespace suez {

typedef int32_t FullVersion;
typedef int32_t IncVersion;

struct TableId : public autil::legacy::Jsonizable {
    TableId() : fullVersion(-1) {}
    std::string tableName;
    FullVersion fullVersion;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("table_name", tableName, tableName);
        json.Jsonize("full_version", fullVersion, fullVersion);
    }
};

typedef int32_t RangeType;
struct PartitionId : public autil::legacy::Jsonizable {
    PartitionId() : from(-1), to(-1) {}
    TableId tableId;
    RangeType from;
    RangeType to;
    int32_t index = -1;
    int32_t partCount = -1;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("table_id", tableId, tableId);
        json.Jsonize("from", from, from);
        json.Jsonize("to", to, to);
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("index", index, index);
            json.Jsonize("partCount", partCount, partCount);
        } else {
            if (index >= 0) {
                json.Jsonize("index", index);
            }
            if (partCount > 0) {
                json.Jsonize("partCount", partCount);
            }
        }
    }

    const std::string &getTableName() const { return tableId.tableName; }

    bool isSameRange(const PartitionId &partitionId) const {
        return partitionId.from == this->from && partitionId.to == this->to;
    }

    bool isSameTable(const PartitionId &partitionId) const {
        return isSameRange(partitionId) && partitionId.tableId.tableName == tableId.tableName;
    }

    FullVersion getFullVersion() const { return tableId.fullVersion; }

    void setPartitionIndex(int32_t partCount_);

    bool validate() const { return index != -1 && partCount != -1; }

    bool covers(RangeType hashId) const { return from <= hashId && hashId <= to; }
};

template <typename T1, typename T2>
std::set<T1> mapKeys2Set(const std::map<T1, T2> &m) {
    std::set<T1> s;
    for (typename std::map<T1, T2>::const_iterator it = m.begin(); it != m.end(); ++it) {
        s.insert(it->first);
    }
    return s;
}

template <typename T>
std::set<T> setDiff(const std::set<T> &left, const std::set<T> &right) {
    std::set<T> result;
    std::set_difference(left.begin(), left.end(), right.begin(), right.end(), std::inserter(result, result.end()));
    return result;
}

template <typename T>
std::set<T> setUnion(const std::set<T> &left, const std::set<T> &right) {
    std::set<T> result;
    std::set_union(left.begin(), left.end(), right.begin(), right.end(), std::inserter(result, result.end()));
    return result;
}

bool operator<(const TableId &left, const TableId &right);
bool operator==(const TableId &left, const TableId &right);
bool operator!=(const TableId &left, const TableId &right);

bool operator<(const PartitionId &left, const PartitionId &right);
bool operator==(const PartitionId &left, const PartitionId &right);
bool operator!=(const PartitionId &left, const PartitionId &right);

} // namespace suez
