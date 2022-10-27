#ifndef __INDEXLIB_INDEX_APPLICATION_H
#define __INDEXLIB_INDEX_APPLICATION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/partition_define.h"
#include "indexlib/partition/table_reader_container.h"
#include "indexlib/partition/join_cache/join_field.h"
#include <unordered_map>

DECLARE_REFERENCE_CLASS(partition, TableReaderContainerUpdater);
DECLARE_REFERENCE_CLASS(partition, PartitionReaderSnapshot);
DECLARE_REFERENCE_CLASS(partition, IndexPartition);
DECLARE_REFERENCE_CLASS(partition, OfflinePartition);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(partition);


class IndexApplication
{
public:
    // tableName --> IndexPartition
    typedef std::map<std::string, IndexPartitionPtr> IndexPartitionMap;
    // first: tableName, second: data ts
    typedef std::pair<std::string, int64_t> IndexDataTimestampInfo;
public:
    IndexApplication();
    ~IndexApplication();
    
public:
    bool Init(const std::vector<IndexPartitionPtr> &indexPartitions,
              const JoinRelationMap &joinRelations);

    // TODO: remain for legacy interface caller
    bool Init(const IndexPartitionMap &indexPartitions,
              const JoinRelationMap &joinRelations);
    // for single offline partition reader
    bool Init(const std::vector<OfflinePartitionPtr>& indexPartitions);    
    PartitionReaderSnapshotPtr CreateSnapshot();
    PartitionReaderSnapshotPtr CreateSnapshot(const std::string& leadingTableName);
    void GetTableSchemas(
            std::vector<config::IndexPartitionSchemaPtr>& tableSchemas);
    config::IndexPartitionSchemaPtr GetTableSchema(
        const std::string& tableName) const; 
    void GetTableLatestDataTimestamps(
            std::vector<IndexDataTimestampInfo>& dataTsInfos);

    bool GetLatestDataTimestamp(const std::string& tableName, int64_t& dataTs);

    bool IsLatestSnapshotReader(
        const PartitionReaderSnapshotPtr& snapshotReader) const;

    bool UpdateSnapshotReader(
        const PartitionReaderSnapshotPtr& snapshotReader, const std::string& tableName) const;

    bool IsLatestPartitionReader(
        const IndexPartitionReaderPtr& partitionReader) const;

private:
    bool AddIndexPartitions(const std::vector<IndexPartitionPtr> &indexPartitions);
    bool AddIndexPartitionsFromVec();
    bool AddIndexPartition(const config::IndexPartitionSchemaPtr &schema,
                           uint32_t id);
    // bool ParseJoinRelations(const JoinRelationMap &joinRelations);
    // bool ParseJoinRelation(const std::string &mainTableName,
    //                        const std::vector<JoinField> &joinFields);
    void ReviseReaderType(const std::string &tableName, 
                          const std::string &joinFieldName);
    void RevisePrimaryReaderId(const std::string& mainTableName,
                               const std::string& joinTableName,
                               const std::string &joinFieldName);

private:
    std::vector<IndexPartitionPtr> mIndexPartitionVec;
    std::vector<IndexPartitionReaderType> mReaderTypeVec;
    std::unordered_map<std::string, uint32_t> mTableName2PartitionIdMap;
    TableMem2IdMap mIndexName2IdMap;
    TableMem2IdMap mAttrName2IdMap;
    TableMem2IdMap mPackAttrName2IdMap;
    TableReaderContainer mReaderContainer;
    TableReaderContainerUpdaterPtr mReaderUpdater;
private:
    friend class IndexApplicationTest;
    friend class ReaderSnapshotTest;
    friend class AuxTableInteTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexApplication);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEX_APPLICATION_H
