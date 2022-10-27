#ifndef __INDEXLIB_PARTITION_READER_SNAPSHOT_H
#define __INDEXLIB_PARTITION_READER_SNAPSHOT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/partition_define.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/aux_table_reader_creator.h"
#include "indexlib/partition/join_cache/join_info.h"
#include "indexlib/partition/join_cache/join_field.h"

IE_NAMESPACE_BEGIN(partition);

class PartitionReaderSnapshot
{
public:
    PartitionReaderSnapshot(
            const TableMem2IdMap *indexName2IdMap,
            const TableMem2IdMap *attrName2IdMap,
            const TableMem2IdMap *packAttrName2IdMap,
            const std::vector<IndexPartitionReaderInfo> &indexPartReaderInfos,
            const ReverseJoinRelationMap* reverseJoinRelations = NULL,
            const std::string& leadingTableName = "");

    ~PartitionReaderSnapshot();
public:
    index::IndexReaderPtr GetIndexReader(const std::string &tableName,
            const std::string &indexName) const;
    
    bool GetAttributeReaderInfo(
            const std::string &attrName,
            const std::string &tableName,
            AttributeReaderInfo &attrReaderInfo) const;

    bool GetAttributeReaderInfo(
            const std::string &attrName,
            const std::string &tableName,
            RawPointerAttributeReaderInfo &attrReaderInfo) const;

    bool GetIndexPartitionReaderInfo(
            const std::string &attrName,
            const std::string &tableName,
            IndexPartitionReaderInfo &indexPartReaderInfo) const;

    const IndexPartitionReaderInfo* GetIndexPartitionReaderInfo(
            const std::string &attrName,
            const std::string &tableName) const;

    bool GetAttributeJoinInfo(const std::string& joinTableName,
                              const std::string& mainTableName,
                              JoinInfo &joinInfo) const;

    bool GetPackAttributeReaderInfo(
            const std::string &packAttrName,
            const std::string &tableName,
            PackAttributeReaderInfo &attrReaderInfo) const;

    std::string GetTableNameByAttribute(const std::string &attrName,
            const std::string &preferTableName);

    std::string GetTableNameByPackAttribute(const std::string &packAttrName,
            const std::string &preferTableName);
    
    // TODO: support pack attribute
    template <typename T>
    AuxTableReaderTyped<T> *CreateAuxTableReader(
            const std::string &attrName,
            const std::string &tableName,
            autil::mem_pool::Pool *pool) const
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

    const IndexPartitionReaderPtr& GetIndexPartitionReader(const std::string& tableName) const
    {
        Name2IdMap::const_iterator iter = mTableName2Idx.find(tableName);
        if (iter == mTableName2Idx.end())
        {
            static IndexPartitionReaderPtr NULL_READER;
            return NULL_READER;
        }
        const IndexPartitionReaderInfo& readerInfo = mIndexPartReaderInfos[iter->second];
        assert(readerInfo.tableName == tableName);
        return readerInfo.indexPartReader;
    }

    const IndexPartitionReaderPtr& GetIndexPartitionReader(uint32_t idx)
    { return mIndexPartReaderInfos[idx].indexPartReader; }

    void UpdateIndexPartitionReaderInfo(uint32_t idx, const IndexPartitionReaderInfo& readerInfo)
    { mIndexPartReaderInfos[idx] = readerInfo; }

    PartitionReaderSnapshot* CreateLeadingSnapshotReader(const std::string& leadingTableName);

private:
    bool InitAttributeJoinInfo(
        const std::string& mainTableName,
        const std::string& joinTableName,
        const std::string& joinKey,
        const JoinField& joinField,
        JoinInfo& joinInfo) const;
    void InitName2IdxMap(const TableMem2IdMap* member2IdMap,
                         Name2IdMap& name2IdMap);
    bool GetIdxByName(const std::string& name, const Name2IdMap& map, uint32_t& idx) const;

    bool GetTableNameForAttribute(const std::string &attrName,
                                  const std::string &tableName) const;

    bool IsLeadingRelatedTable(const std::string& tableName) const;
                       
    bool IsJoinTable(const std::string& joinTableName,
                     const std::string& mainTableName) const;

    const IndexPartitionReaderInfo& GetIndexPartitionReaderInfo(const std::string& tableName) const
    {
        Name2IdMap::const_iterator iter = mTableName2Idx.find(tableName);
        if (iter == mTableName2Idx.end())
        {
            static IndexPartitionReaderInfo NULL_INFO;
            return NULL_INFO;
        }
        return mIndexPartReaderInfos[iter->second];
    }

    bool GetReaderIdByAttrNameAndTableName(const std::string &attrName,
            const std::string &tableName, uint32_t &readerId) const;
    
private:
    const TableMem2IdMap* mIndex2IdMap;
    const TableMem2IdMap* mAttribute2IdMap;
    const TableMem2IdMap* mPackAttribute2IdMap;
    std::vector<IndexPartitionReaderInfo> mIndexPartReaderInfos;
    // TODO: legacy, remove later
    // std::tr1::unordered_map<std::string, IndexPartitionReader*> mPartitions;
    const ReverseJoinRelationMap* mReverseJoinRelations;
    Name2IdMap mTableName2Idx;
    Name2IdMap mIndexName2Idx;
    Name2IdMap mAttributeName2Idx;
    Name2IdMap mPackAttributeName2Idx;
    const std::string mLeadingTable;

private:
    friend class IndexApplicationTest;
    friend class ReaderSnapshotTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionReaderSnapshot);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_READER_SNAPSHOT_H
