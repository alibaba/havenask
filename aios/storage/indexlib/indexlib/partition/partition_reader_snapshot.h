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
#ifndef __INDEXLIB_PARTITION_READER_SNAPSHOT_H
#define __INDEXLIB_PARTITION_READER_SNAPSHOT_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/aux_table_reader_creator.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "indexlib/partition/join_cache/join_info.h"
#include "indexlib/partition/partition_define.h"

namespace indexlibv2::framework {
class TabletInfos;
}
namespace indexlib { namespace partition {

struct TableMainSubIdx {
    std::map<std::string, uint32_t> indexName2IdMap;
    std::map<std::string, uint32_t> attrName2IdMap;
    std::vector<IndexPartitionReaderPtr> indexReaderVec;
};

class PartitionReaderSnapshot
{
public:
    PartitionReaderSnapshot();
    PartitionReaderSnapshot(const TableMem2IdMap* indexName2IdMap, const TableMem2IdMap* attrName2IdMap,
                            const TableMem2IdMap* packAttrName2IdMap,
                            const std::vector<IndexPartitionReaderInfo>& indexPartReaderInfos,
                            const ReverseJoinRelationMap* reverseJoinRelations = NULL,
                            const std::string& leadingTableName = "");
    // for tablet
    PartitionReaderSnapshot(const TableMem2IdMap* indexName2IdMap, const TableMem2IdMap* attrName2IdMap,
                            const TableMem2IdMap* packAttrName2IdMap,
                            const std::vector<IndexPartitionReaderInfo>& indexPartReaderInfos,
                            const ReverseJoinRelationMap* reverseJoinRelations,
                            const std::vector<TabletReaderInfo>& tabletReaderInfos,
                            const std::string& leadingTableName);

    virtual ~PartitionReaderSnapshot();

public:
    std::shared_ptr<index::InvertedIndexReader> GetInvertedIndexReader(const std::string& tableName,
                                                                       const std::string& indexName) const;

    bool GetAttributeReaderInfoV1(const std::string& attrName, const std::string& tableName,
                                  AttributeReaderInfo& attrReaderInfo) const;
    // for tablet
    bool GetAttributeReaderInfoV2(const std::string& attrName, const std::string& tableName,
                                  AttributeReaderInfoV2& attrReaderInfo) const;

    bool GetAttributeReaderInfoV1(const std::string& attrName, const std::string& tableName,
                                  RawPointerAttributeReaderInfo& attrReaderInfo) const;

    virtual bool GetIndexPartitionReaderInfo(const std::string& attrName, const std::string& tableName,
                                             IndexPartitionReaderInfo& indexPartReaderInfo) const;

    bool GetTabletReaderInfo(const std::string& attrName, const std::string& tableName,
                             TabletReaderInfo& tabletReaderInfos) const;

    const IndexPartitionReaderInfo* GetIndexPartitionReaderInfo(const std::string& attrName,
                                                                const std::string& tableName) const;

    bool GetAttributeJoinInfo(const std::string& joinTableName, const std::string& mainTableName,
                              JoinInfo& joinInfo) const;

    bool GetPackAttributeReaderInfo(const std::string& packAttrName, const std::string& tableName,
                                    PackAttributeReaderInfo& attrReaderInfo) const;

    // for tbalet
    bool GetPackAttributeReaderInfo(const std::string& packAttrName, const std::string& tableName,
                                    PackAttributeReaderInfoV2& attrReaderInfo) const;

    std::string GetTableNameByAttribute(const std::string& attrName, const std::string& preferTableName);

    std::string GetTableNameByPackAttribute(const std::string& packAttrName, const std::string& preferTableName);

    // TODO: support pack attribute
    template <typename T>
    AuxTableReaderTyped<T>* CreateAuxTableReader(const std::string& attrName, const std::string& tableName,
                                                 autil::mem_pool::Pool* pool) const
    {
        IndexPartitionReaderInfo readerInfo;
        if (!GetIndexPartitionReaderInfo(attrName, tableName, readerInfo)) {
            return nullptr;
        }
        if (!readerInfo.indexPartReader) {
            return nullptr;
        }
        return AuxTableReaderCreator::Create<T>(readerInfo.indexPartReader, attrName, pool);
    }

    virtual const IndexPartitionReaderPtr& GetIndexPartitionReader(const std::string& tableName) const
    {
        static IndexPartitionReaderPtr NULL_READER;
        Name2IdMap::const_iterator iter = mTableName2Idx.find(tableName);
        if (iter == mTableName2Idx.end()) {
            return NULL_READER;
        }
        if (iter->second >= mIndexPartReaderInfos.size()) {
            return NULL_READER;
        }
        const IndexPartitionReaderInfo& readerInfo = mIndexPartReaderInfos[iter->second];
        assert(readerInfo.tableName == tableName);
        return readerInfo.indexPartReader;
    }

    const IndexPartitionReaderPtr& GetIndexPartitionReader(uint32_t idx)
    {
        if (idx >= mIndexPartReaderInfos.size()) {
            static IndexPartitionReaderPtr NULL_READER;
            return NULL_READER;
        }
        return mIndexPartReaderInfos[idx].indexPartReader;
    }

    virtual bool tableName2SchemaMap(
        std::unordered_map<std::string, std::pair<bool, indexlib::config::IndexPartitionSchemaPtr>>& schemaMap);

    void UpdateIndexPartitionReaderInfo(uint32_t idx, const IndexPartitionReaderInfo& readerInfo)
    {
        if (idx < mIndexPartReaderInfos.size()) {
            mIndexPartReaderInfos[idx] = readerInfo;
        }
    }

    PartitionReaderSnapshot* CreateLeadingSnapshotReader(const std::string& leadingTableName);

    // TODO tmp store table main/sub index, refator laster
    void InitTableMainSubIdxMap();
    const std::unordered_map<std::string, TableMainSubIdx>& getTableMainSubIdxMap() const
    {
        return mTableMainSubIdxMap;
    }

    virtual const std::shared_ptr<indexlibv2::framework::ITabletReader>&
    GetTabletReader(const std::string& tableName) const
    {
        Name2IdMap::const_iterator iter = mTableName2Idx.find(tableName);
        static std::shared_ptr<indexlibv2::framework::ITabletReader> NULL_READER(nullptr);
        if (iter == mTableName2Idx.end()) {
            return NULL_READER;
        }
        auto idx = iter->second;
        if (idx < mIndexPartReaderInfos.size()) {
            // IndexPartitionReader
            return NULL_READER;
        }
        idx = idx - mIndexPartReaderInfos.size();
        assert(idx < _tabletReaderInfos.size());
        const auto& readerInfo = _tabletReaderInfos[idx];
        assert(readerInfo.tableName == tableName);
        return readerInfo.tabletReader;
    }

    const std::shared_ptr<indexlibv2::framework::ITabletReader>& GetTabletReader(uint32_t idx)
    {
        static std::shared_ptr<indexlibv2::framework::ITabletReader> NULL_READER(nullptr);
        if (idx < mIndexPartReaderInfos.size()) {
            // IndexPartitionReader Zero
            return NULL_READER;
        }
        idx = idx - mIndexPartReaderInfos.size();
        assert(idx < _tabletReaderInfos.size());
        return _tabletReaderInfos[idx].tabletReader;
    }
    bool isLeader(const std::string& tableName) const;

private:
    bool InitAttributeJoinInfo(const std::string& mainTableName, const std::string& joinTableName,
                               const std::string& joinKey, const JoinField& joinField, JoinInfo& joinInfo) const;
    void InitName2IdxMap(const TableMem2IdMap* member2IdMap, Name2IdMap& name2IdMap);
    bool GetIdxByName(const std::string& name, const Name2IdMap& map, uint32_t& idx) const;

    bool GetTableNameForAttribute(const std::string& attrName, const std::string& tableName) const;

    bool IsLeadingRelatedTable(const std::string& tableName) const;

    bool IsJoinTable(const std::string& joinTableName, const std::string& mainTableName) const;

    const IndexPartitionReaderInfo& GetIndexPartitionReaderInfo(const std::string& tableName) const
    {
        static IndexPartitionReaderInfo NULL_INFO;
        Name2IdMap::const_iterator iter = mTableName2Idx.find(tableName);
        if (iter == mTableName2Idx.end()) {
            return NULL_INFO;
        }
        if (iter->second >= mIndexPartReaderInfos.size()) {
            return NULL_INFO;
        }
        return mIndexPartReaderInfos[iter->second];
    }

    bool GetReaderIdByAttrNameAndTableName(const std::string& attrName, const std::string& tableName,
                                           uint32_t& readerId) const;

    // TODO tmp store table main/sub index, refator laster
    static void AddMainSubIdxFromSchema(const config::IndexPartitionSchemaPtr& schemaPtr, uint32_t id,
                                        std::map<std::string, uint32_t>& indexName2IdMap,
                                        std::map<std::string, uint32_t>& attrName2IdMap);

private:
    const TableMem2IdMap* mIndex2IdMap;
    const TableMem2IdMap* mAttribute2IdMap;
    const TableMem2IdMap* mPackAttribute2IdMap;
    std::vector<IndexPartitionReaderInfo> mIndexPartReaderInfos;
    std::vector<TabletReaderInfo> _tabletReaderInfos;
    // TODO: legacy, remove later
    // std::unordered_map<std::string, IndexPartitionReader*> mPartitions;
    const ReverseJoinRelationMap* mReverseJoinRelations;
    Name2IdMap mTableName2Idx;
    Name2IdMap mIndexName2Idx;
    Name2IdMap mAttributeName2Idx;
    Name2IdMap mPackAttributeName2Idx;
    const std::string mLeadingTable;
    std::unordered_map<std::string, TableMainSubIdx> mTableMainSubIdxMap;
    std::unordered_map<std::string, bool> mTabletLeaderInfos;
    std::unordered_map<std::string, const indexlibv2::framework::TabletInfos*> mTabletLocatorInfos;

private:
    friend class IndexApplicationTest;
    friend class ReaderSnapshotTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionReaderSnapshot);
}} // namespace indexlib::partition

#endif //__INDEXLIB_PARTITION_READER_SNAPSHOT_H
