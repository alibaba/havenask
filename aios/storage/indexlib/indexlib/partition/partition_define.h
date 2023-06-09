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
#ifndef __INDEXLIB_PARTITION_DEFINE_H
#define __INDEXLIB_PARTITION_DEFINE_H

#include <map>
#include <unordered_map>

#include "autil/ConstString.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);

namespace indexlibv2::framework {
class ITabletReader;
class TabletInfos;
} // namespace indexlibv2::framework
namespace indexlibv2::index {
class AttributeReader;
}

namespace indexlib { namespace partition {

enum IndexPartitionReaderType {
    READER_UNKNOWN_TYPE,
    READER_PRIMARY_MAIN_TYPE,
    READER_PRIMARY_SUB_TYPE,
};

struct IndexPartitionReaderInfo {
    IndexPartitionReaderType readerType;
    IndexPartitionReaderPtr indexPartReader;
    std::string tableName;
    uint64_t partitionIdentifier;
};

struct TabletReaderInfo {
    IndexPartitionReaderType readerType;
    std::shared_ptr<indexlibv2::framework::ITabletReader> tabletReader;
    std::string tableName;
    uint64_t partitionIdentifier;
    bool isLeader = false;
    const indexlibv2::framework::TabletInfos* tabletInfos = nullptr;
};

struct AttributeReaderInfo {
    index::AttributeReaderPtr attrReader;
    IndexPartitionReaderPtr indexPartReader;
    IndexPartitionReaderType indexPartReaderType;
};

struct AttributeReaderInfoV2 {
    std::shared_ptr<indexlibv2::index::AttributeReader> attrReader;
    std::shared_ptr<indexlibv2::framework::ITabletReader> tabletReader;
    IndexPartitionReaderType indexPartReaderType;
};

struct PackAttributeReaderInfo {
    index::PackAttributeReaderPtr packAttrReader;
    IndexPartitionReaderPtr indexPartReader;
    IndexPartitionReaderType indexPartReaderType;
};

struct RawPointerAttributeReaderInfo {
    index::AttributeReader* attrReader = nullptr;
    IndexPartitionReader* indexPartReader = nullptr;
    IndexPartitionReaderType indexPartReaderType;
};

class TableMem2IdMap
{
public:
    typedef std::map<std::pair<std::string, std::string>, uint32_t> IdentityMap;
    typedef IdentityMap::const_iterator Iterator;

public:
    TableMem2IdMap() {}
    ~TableMem2IdMap() {}

public:
    bool Exist(const std::string& tableName, const std::string& id) const
    {
        auto iter = mIdMap.find(std::make_pair(tableName, id));
        return iter != mIdMap.end();
    }

    bool Find(const std::string& tableName, const std::string& id, uint32_t& value) const
    {
        IdentityMap::const_iterator iter = mIdMap.find(std::make_pair(tableName, id));
        if (iter == mIdMap.end()) {
            return false;
        }
        value = iter->second;
        return true;
    }

    uint32_t FindValueWithDefault(const std::string& tableName, const std::string& id, uint32_t defaultValue = 0) const
    {
        uint32_t ret;
        bool success = Find(tableName, id, ret);
        if (!success) {
            return defaultValue;
        }
        return ret;
    }

    void Insert(const std::string& tableName, const std::string& id, uint32_t value)
    {
        auto key = std::make_pair(tableName, id);
        IdentityMap::iterator iter = mIdMap.find(key);
        if (iter != mIdMap.end()) {
            iter->second = value;
        } else {
            mIdMap.insert(std::make_pair(key, value));
        }
    }

    Iterator Begin() const { return mIdMap.begin(); }
    Iterator End() const { return mIdMap.end(); }
    size_t Size() const { return mIdMap.size(); }

private:
    IdentityMap mIdMap;
};

typedef std::unordered_map<std::string, uint32_t> Name2IdMap;
}} // namespace indexlib::partition

#endif //__INDEXLIB_PARTITION_DEFINE_H
