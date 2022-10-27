#ifndef __INDEXLIB_TABLE_READER_CONTAINER_UPDATER_H
#define __INDEXLIB_TABLE_READER_CONTAINER_UPDATER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/join_cache/join_field.h"
#include <unordered_map>

DECLARE_REFERENCE_CLASS(partition, TableReaderContainer);
DECLARE_REFERENCE_CLASS(partition, IndexPartition);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(partition, OnlinePartition);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

IE_NAMESPACE_BEGIN(partition);

class TableReaderContainerUpdater
{
public:
    typedef std::pair<JoinField, uint32_t> AuxTableInfo;
public:
    TableReaderContainerUpdater();
    ~TableReaderContainerUpdater();
public:
    bool IsAuxTable(const std::string& schemaName)
    {
        return mReverseJoinRelationMap.find(schemaName) !=
            mReverseJoinRelationMap.end();
    }
    bool HasAuxTable(const std::string& schemaName)
    {
        return mTableToVirutalAttributes.find(schemaName) != mTableToVirutalAttributes.end();
    }
    bool Init(const std::vector<IndexPartitionPtr>& indexPartitionVec,
              const JoinRelationMap& joinRelations,
              TableReaderContainer* readerContainer);
    bool Update(const IndexPartitionReaderPtr& indexPartReader);
    const ReverseJoinRelationMap* GetJoinRelationMap() const { return &mJoinRelationMap; }
    void FillVirtualAttributeConfigs(const std::string& tableName,
                              std::vector<config::AttributeConfigPtr>& mainAttributeConfigs,
                              std::vector<config::AttributeConfigPtr>& subAttributeConfigs);

private:
    bool AddAllVirtualAttributes();
    void AddVirtualAttributes(partition::OnlinePartition* mainPartition);
    bool ReserveAllVirtualAttributeMemory(std::vector<util::MemoryReserverPtr>& reserverVec);
    bool ReserveVirtualMemoryForOneTable(
        const std::string& mainTableName,
        std::vector<util::MemoryReserverPtr>& reserverVec);
    void CollectAllVirtualAttributeConfigs(const JoinRelationMap& joinRelations);
    void CollectOneTableVirtualAttributeConfigs(
            const std::string &mainTableName,
            const std::vector<JoinField> &joinFields);
    bool CollectToUpdateMainTables(
            const IndexPartitionReaderPtr& indexPartReader,
            std::vector<int32_t>& tableIdxVec);
    config::AttributeConfigPtr CreateAttributeConfig(
            partition::OnlinePartition* mainPart,
            const std::string& auxTableName,
            const std::string& joinField,  bool& isSub);
    void LockAll();
    void UnLockAll();
    void Lock(const std::vector<int32_t>& mainTableIdxVec);
    void UnLock(const std::vector<int32_t>& mainTableIdxVec);
    void UpdateVirtualAttrConfigs(
            IndexPartitionReaderPtr auxPartReader,
            const std::vector<int32_t>& mainTableIdxVec);
    bool InitPartitions(
        const std::vector<IndexPartitionPtr>& indexPartitionVec,
        const JoinRelationMap &joinRelations);
    bool ParseJoinRelations(const JoinRelationMap &joinRelations);
    bool ParseJoinRelation(partition::OnlinePartition* mainPartition,
                           const std::vector<JoinField> &joinFields);
    bool HasAttribute(partition::IndexPartition* partition,
                      const std::string& fieldName, bool& isSub);
    void UpdateReaders();

public:
    partition::IndexPartition* GetIndexPartition(const std::string& tableName)
    {
        int32_t idx = GetIndexPartitionIdx(tableName);
        if (idx == -1)
        {
            return NULL;
        }
        return mIndexPartitions[idx];
    }
    int32_t GetIndexPartitionIdx(const std::string& tableName)
    {
        auto iter = mTableName2PartitionIdMap.find(tableName);
        if (iter == mTableName2PartitionIdMap.end())
        {
            return -1;
        }
        return (int32_t)iter->second;
    }

private:
    struct VirtualAttributeInfo
    {
        VirtualAttributeInfo(const std::string& _auxTable, bool _isSub,
                             const config::AttributeConfigPtr& _attrConfig)
            : auxTableName(_auxTable)
            , isSub(_isSub)
            , attrConfig(_attrConfig)
        {
        }
        std::string auxTableName;
        bool isSub;
        config::AttributeConfigPtr attrConfig;
    };
    std::unordered_map<std::string, uint32_t> mTableName2PartitionIdMap;
    ReverseJoinRelationMap mReverseJoinRelationMap;
    ReverseJoinRelationMap mJoinRelationMap;
    std::vector<partition::IndexPartition*> mIndexPartitions;
    TableReaderContainer* mReaderContainer;
    std::unordered_map<std::string, std::vector<VirtualAttributeInfo> > mTableToVirutalAttributes;
    
private:
    friend class TableReaderContainerUpdaterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableReaderContainerUpdater);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_TABLE_READER_CONTAINER_UPDATER_H
