#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/partition/table_reader_container.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/join_cache/join_cache_initializer_creator.h"
#include "indexlib/config/virtual_attribute_config_creator.h"
#include "indexlib/util/memory_control/memory_reserver.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, TableReaderContainerUpdater);

TableReaderContainerUpdater::TableReaderContainerUpdater() 
{
}

TableReaderContainerUpdater::~TableReaderContainerUpdater() 
{
    LockAll();
    for (size_t i = 0; i < mIndexPartitions.size(); i++)
    {
        auto tableName = mIndexPartitions[i]->GetSchema()->GetSchemaName();
        mIndexPartitions[i]->RemoveReaderUpdater(this);
        if (mIndexPartitions[i]->GetSchema()->GetTableType() == TableType::tt_customized)
        {
            continue;
        }
        OnlinePartition* indexPart = dynamic_cast<OnlinePartition*>(mIndexPartitions[i]);
        assert(indexPart);
        auto iter = mTableToVirutalAttributes.find(tableName);
        if (iter != mTableToVirutalAttributes.end()
            && !iter->second.empty())
        {
            vector<AttributeConfigPtr> mainAttributeConfigs;
            vector<AttributeConfigPtr> subAttributeConfigs;
            indexPart->AddVirtualAttributes(
                mainAttributeConfigs, subAttributeConfigs);
        }
    }
    UnLockAll();
}

bool TableReaderContainerUpdater::Init(const std::vector<IndexPartitionPtr>& indexPartitionVec,
          const JoinRelationMap& joinRelations,
          TableReaderContainer* readerContainer)
{
    if (!InitPartitions(indexPartitionVec, joinRelations))
    {
        return false;
    }
    if (!ParseJoinRelations(joinRelations))
    {
        return false;
    }
    
    LockAll();
    mReaderContainer = readerContainer;
    CollectAllVirtualAttributeConfigs(joinRelations);
    vector<MemoryReserverPtr> reserverVec;

    if (!ReserveAllVirtualAttributeMemory(reserverVec)
        || !AddAllVirtualAttributes())
    {
        UnLockAll();
        return false;
    }
    for (size_t i = 0; i < mIndexPartitions.size(); i++)
    {
        mIndexPartitions[i]->AddReaderUpdater(this);        
        mReaderContainer->UpdateReader(
                mIndexPartitions[i]->GetSchema()->GetSchemaName(),
                mIndexPartitions[i]->GetReader());
    }
    UnLockAll();
    return true;
}

bool TableReaderContainerUpdater::Update(const IndexPartitionReaderPtr& indexPartReader)
{
    vector<int32_t> mainTableIdxVec;
    if (!CollectToUpdateMainTables(indexPartReader, mainTableIdxVec)) {
        return false;
    }
    string tableName = indexPartReader->GetSchema()->GetSchemaName();
    if (mainTableIdxVec.size() == 0)
    {
        mReaderContainer->UpdateReader(tableName, indexPartReader);
        return true;
    }

    Lock(mainTableIdxVec);

    vector<string> tableNames;
    std::vector<IndexPartitionReaderPtr> readers;
    tableNames.push_back(tableName);
    readers.push_back(indexPartReader);

    UpdateVirtualAttrConfigs(indexPartReader, mainTableIdxVec);
    vector<MemoryReserverPtr> partMemoryReservers;
    for (size_t i = 0; i < mainTableIdxVec.size(); i ++)
    {
        auto partitionReader = mIndexPartitions[mainTableIdxVec[i]];
        string mainTableName = partitionReader->GetSchema()->GetSchemaName();
        if (!ReserveVirtualMemoryForOneTable(mainTableName, partMemoryReservers))
        {
            IE_LOG(ERROR, "reserve virtual mem for table[%s] failed",
                   mainTableName.c_str());
            UnLock(mainTableIdxVec);
            return false;
        }
    }

    try
    {
        for (size_t i = 0; i < mainTableIdxVec.size(); i++)
        {
            OnlinePartition* mainPartition =
                dynamic_cast<OnlinePartition*>(mIndexPartitions[mainTableIdxVec[i]]);
            assert(mainPartition);
            AddVirtualAttributes(mainPartition);
            tableNames.push_back(mainPartition->GetSchema()->GetSchemaName());
            readers.push_back(mainPartition->GetReader());
        }
    }
    catch(...)
    {
        IE_LOG(ERROR, "add virtual attributes exception!!");
        UnLock(mainTableIdxVec);
        return false;
    }
    mReaderContainer->UpdateReaders(tableNames, readers);
    UnLock(mainTableIdxVec);
    return true;
}

bool TableReaderContainerUpdater::AddAllVirtualAttributes()
{
    try
    {
        // make sure no customized table is in mTableToVirutalAttributes
        auto iter = mTableToVirutalAttributes.begin();
        for (; iter != mTableToVirutalAttributes.end(); iter++)
        {
            if (!iter->second.empty())
            {
                OnlinePartition* mainPartition =
                    dynamic_cast<OnlinePartition*>(GetIndexPartition(iter->first));
                assert(mainPartition);
                AddVirtualAttributes(mainPartition);
            }
        }
    }
    catch (...)
    {
        IE_LOG(ERROR, "add virtual attributes exception!!");
        return false;
    }
    return true;
}

bool TableReaderContainerUpdater::ReserveAllVirtualAttributeMemory(
    vector<MemoryReserverPtr>& reserverVec)
{
    auto iter = mTableToVirutalAttributes.begin();
    for (; iter != mTableToVirutalAttributes.end(); iter++)
    {
        if (!ReserveVirtualMemoryForOneTable(iter->first, reserverVec))
        {
            return false;
        }
    }
    return true;
}

bool TableReaderContainerUpdater::ReserveVirtualMemoryForOneTable(
        const std::string& mainTableName,
        vector<MemoryReserverPtr>& reserverVec)
{
    vector<AttributeConfigPtr> mainAttrConfigs;
    vector<AttributeConfigPtr> subAttrConfigs;
    FillVirtualAttributeConfigs(mainTableName, mainAttrConfigs, subAttrConfigs);
    OnlinePartition* indexPart = dynamic_cast<OnlinePartition*>(GetIndexPartition(mainTableName));
    assert(indexPart);
    MemoryReserverPtr reserver =
        indexPart->ReserveVirtualAttributesResource(mainAttrConfigs, subAttrConfigs);
    if (!reserver)
    {
        reserverVec.clear();
        return false;
    }
    reserverVec.push_back(reserver);
    return true;
}

void TableReaderContainerUpdater::CollectAllVirtualAttributeConfigs(const JoinRelationMap& joinRelations)
{
    JoinRelationMap::const_iterator iter = joinRelations.begin();
    for (; iter != joinRelations.end(); iter++)
    {
        CollectOneTableVirtualAttributeConfigs(iter->first, iter->second);
    }
}

void TableReaderContainerUpdater::CollectOneTableVirtualAttributeConfigs(const string &mainTableName,
        const vector<JoinField> &joinFields)
{
    for (size_t i = 0; i < joinFields.size(); i++)
    {
        if (!joinFields[i].genJoinCache)
        {
            continue;
        }
        const string &joinTableName = joinFields[i].joinTableName;
        const string &joinFieldName = joinFields[i].fieldName;
        IndexPartition* indexPartition = GetIndexPartition(mainTableName);
        // customized table will not support virtual attribute & aux table
        if (indexPartition->GetSchema()->GetTableType() == TableType::tt_customized)
        {
            continue;
        }
        OnlinePartition* mainPartition = dynamic_cast<OnlinePartition*>(indexPartition);
        assert(mainPartition);
        bool isSub = false;
        HasAttribute(indexPartition, joinFieldName, isSub);
        AttributeConfigPtr attrConfig = CreateAttributeConfig(
                mainPartition, joinTableName, joinFieldName, isSub);
        mTableToVirutalAttributes[mainTableName].push_back(
            VirtualAttributeInfo(joinTableName, isSub, attrConfig));
    }
}

bool TableReaderContainerUpdater::CollectToUpdateMainTables(
    const IndexPartitionReaderPtr& indexPartReader,
    vector<int32_t>& tableIdxVec)
{
    tableIdxVec.clear();
    string tableName = indexPartReader->GetSchema()->GetSchemaName();
    auto iter = mReverseJoinRelationMap.find(tableName);
    if (iter == mReverseJoinRelationMap.end())
    {
        return true;
    }

    IndexPartitionReaderPtr reader = mReaderContainer->GetReader(tableName);
    if (reader->GetIncVersionId() == indexPartReader->GetIncVersionId())
    {
        return true;
    }

    auto joinFieldMapIter = iter->second.begin();    
    for(;  joinFieldMapIter != iter->second.end(); joinFieldMapIter++)
    {
        if (joinFieldMapIter->second.genJoinCache)
        {
            size_t mainTableId = mTableName2PartitionIdMap[joinFieldMapIter->first];
            if (mIndexPartitions[mainTableId]->GetSchema()->GetTableType() == TableType::tt_customized)
            {
                string mainTableName = mIndexPartitions[mainTableId]->GetSchema()->GetSchemaName();
                IE_LOG(ERROR, "customized table[%s] can't be update by join table[%s]",
                       mainTableName.c_str(), tableName.c_str());
                return false;
            }
            tableIdxVec.push_back(mainTableId);
        }
    }
    // sort to make sure different thread lock partition in the same order
    sort(tableIdxVec.begin(), tableIdxVec.end());
    return true;
}

AttributeConfigPtr TableReaderContainerUpdater::CreateAttributeConfig(
        OnlinePartition* mainPart,
        const string& auxTableName,
        const string& joinField,  bool& isSub)
{
    HasAttribute(mainPart, joinField, isSub);
    OnlinePartition* auxPart = dynamic_cast<OnlinePartition*>(GetIndexPartition(auxTableName));
    assert(auxPart);
    auto auxReader = auxPart->GetReader();
    versionid_t auxReaderIncVersionId = auxReader->GetIncVersionId();
    string virtualAttributeName = JoinCacheInitializerCreator::GenerateVirtualAttributeName(
            isSub ? BUILD_IN_SUBJOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX
            : BUILD_IN_JOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX,
            auxPart->GetPartitionIdentifier(), joinField, auxReaderIncVersionId);

    JoinCacheInitializerCreatorPtr creator(new JoinCacheInitializerCreator);
    creator->Init(joinField, isSub, mainPart->GetSchema(), auxPart, auxReaderIncVersionId);
    AttributeConfigPtr attrConfig = VirtualAttributeConfigCreator::Create(
            virtualAttributeName, ft_int32, false, StringUtil::toString(INVALID_DOCID), creator);
    return attrConfig;
}

void TableReaderContainerUpdater::LockAll()
{
    for (size_t i = 0; i < mIndexPartitions.size(); i++)
    {
        OnlinePartition* indexPart = dynamic_cast<OnlinePartition*>(mIndexPartitions[i]);
        // customized online partition not support lock 
        if (indexPart)
        {
            indexPart->Lock();
        }
    }
}

void TableReaderContainerUpdater::UnLockAll()
{
    for (int32_t i = mIndexPartitions.size() - 1; i >= 0; i --)
    {
        OnlinePartition* indexPart = dynamic_cast<OnlinePartition*>(mIndexPartitions[i]);
        // customized online partition not support unlock
        if (indexPart)
        {
            indexPart->UnLock();
        }
    }
}

void TableReaderContainerUpdater::Lock(const vector<int32_t>& mainTableIdxVec)
{
    for (size_t i = 0; i < mainTableIdxVec.size(); i++)
    {
        mIndexPartitions[mainTableIdxVec[i]]->Lock();        
    }
}

void TableReaderContainerUpdater::UnLock(const vector<int32_t>& mainTableIdxVec)
{
    for (int32_t i =  mainTableIdxVec.size() - 1; i >= 0; i--)
    {
        OnlinePartition* indexPart =
            dynamic_cast<OnlinePartition*>(mIndexPartitions[mainTableIdxVec[i]]);
        if (indexPart)
        {
            indexPart->UnLock();
        }
    }
}


void TableReaderContainerUpdater::UpdateVirtualAttrConfigs(
        IndexPartitionReaderPtr auxPartReader,
        const vector<int32_t>& mainTableIdxVec)
{
    auto auxTableName = auxPartReader->GetSchema()->GetSchemaName();
    for (auto &idx: mainTableIdxVec)
    {
        auto schema = mIndexPartitions[idx]->GetSchema();
        string tableName = schema->GetSchemaName();
        string joinFieldName = mReverseJoinRelationMap[auxTableName][tableName].fieldName;
        bool isSub;
        OnlinePartition* indexPartition = dynamic_cast<OnlinePartition*>(mIndexPartitions[idx]);
        assert(indexPartition);
        AttributeConfigPtr attrConfig = CreateAttributeConfig(
                indexPartition, auxTableName, joinFieldName, isSub);
        vector<VirtualAttributeInfo>& virtualAttributeInfos = mTableToVirutalAttributes[tableName];
        for (size_t i = 0; i < virtualAttributeInfos.size(); i++)
        {
            if (virtualAttributeInfos[i].auxTableName == auxTableName)
            {
                virtualAttributeInfos[i] = VirtualAttributeInfo(auxTableName, isSub, attrConfig);
                break;
            }
        }
    }
}

void TableReaderContainerUpdater::FillVirtualAttributeConfigs(const string& tableName,
        vector<AttributeConfigPtr>& mainAttributeConfigs,
        vector<AttributeConfigPtr>& subAttributeConfigs)
{
    partition::OnlinePartition* partition =
        dynamic_cast<OnlinePartition*>(GetIndexPartition(tableName));
    assert(partition);
    partition->Lock();
    vector<VirtualAttributeInfo>& attributeConfigInfos = mTableToVirutalAttributes[tableName];
    for (size_t i = 0; i < attributeConfigInfos.size(); i++)
    {
        if (attributeConfigInfos[i].isSub)
        {
            subAttributeConfigs.push_back(attributeConfigInfos[i].attrConfig);
        }
        else
        {
            mainAttributeConfigs.push_back(attributeConfigInfos[i].attrConfig);
        }
    }
    partition->UnLock();
}

void TableReaderContainerUpdater::AddVirtualAttributes(OnlinePartition* mainPartition)
{
    string tableName = mainPartition->GetSchema()->GetSchemaName();
    vector<AttributeConfigPtr> mainAttributeConfigs;
    vector<AttributeConfigPtr> subAttributeConfigs;
    FillVirtualAttributeConfigs(tableName, mainAttributeConfigs, subAttributeConfigs);
    mainPartition->AddVirtualAttributes(mainAttributeConfigs, subAttributeConfigs);
}

bool TableReaderContainerUpdater::InitPartitions(
        const std::vector<IndexPartitionPtr>& indexPartitionVec,
        const JoinRelationMap &joinRelations)
{
    mIndexPartitions.reserve(indexPartitionVec.size());
    vector<IndexPartition*> mainPartitions;
    for (size_t i = 0; i < indexPartitionVec.size(); i++)
    {
        string tableName = indexPartitionVec[i]->GetSchema()->GetSchemaName();
        if(joinRelations.find(tableName) != joinRelations.end())
        {
            if (indexPartitionVec[i]->GetSchema()->GetTableType() == TableType::tt_customized)
            {
                IE_LOG(ERROR, "customized table[%s] join failed, action not support.",
                       tableName.c_str());
                return false;
            }
            mainPartitions.push_back(indexPartitionVec[i].get());
        }
        else
        {
            mIndexPartitions.push_back(indexPartitionVec[i].get());
        }
    }
    // sort main partition to make sure different updater lock partitions in the same order
    static auto cmp = [](IndexPartition* a, IndexPartition* b) -> bool
                      {
                          return a->GetSchema()->GetSchemaName()
                          < b->GetSchema()->GetSchemaName();
                      };
    std::sort(mainPartitions.begin(), mainPartitions.end(), cmp);
    std::sort(mIndexPartitions.begin(), mIndexPartitions.end(), cmp);
    
    for (size_t i = 0; i < mainPartitions.size(); i ++)
    {
        mIndexPartitions.push_back(mainPartitions[i]);
    }
    for (size_t i = 0; i < mIndexPartitions.size(); i ++)
    {
        string tableName = mIndexPartitions[i]->GetSchema()->GetSchemaName();
        mTableName2PartitionIdMap[tableName] = i;
        auto subSchema = mIndexPartitions[i]->GetSchema()->GetSubIndexPartitionSchema();
        if (subSchema &&
            mIndexPartitions[i]->GetSchema()->GetTableType() != TableType::tt_customized)
        {
            string subTableName = subSchema->GetSchemaName();
            mTableName2PartitionIdMap[subTableName] = i;
        }
    }
    return true;
}

bool TableReaderContainerUpdater::ParseJoinRelations(
        const JoinRelationMap &joinRelations)
{
    JoinRelationMap::const_iterator iter = joinRelations.begin();
    for (; iter != joinRelations.end(); iter++)
    {
        string mainTableName = iter->first;
        IndexPartition* partition = GetIndexPartition(mainTableName);
        if(!partition)
        {
            IE_LOG(ERROR, "no main table [%s]",  mainTableName.c_str());
            return false;
        }

        if (partition->GetSchema()->GetTableType() == TableType::tt_customized)
        {
            IE_LOG(ERROR, "do not support JoinRelation for customized table[%s]",
                   mainTableName.c_str());
            return false;
        }

        OnlinePartition* onlinePart = dynamic_cast<OnlinePartition*>(partition);
        assert(onlinePart);
        if (!ParseJoinRelation(onlinePart, iter->second))
        {
            IE_LOG(ERROR, "parse join relation for table[%s] failed",
                   iter->first.c_str());
            return false;
        }
    }
    return true;
}


bool TableReaderContainerUpdater::HasAttribute(
        IndexPartition* partition,
        const string& fieldName,
        bool& isSub)
{
    IndexPartitionSchemaPtr schema = partition->GetSchema();
    auto subSchema = schema->GetSubIndexPartitionSchema();
    if (schema->GetAttributeSchema()->GetAttributeConfig(fieldName))
    {
        isSub = false;
        return true;
    }
    if (subSchema && subSchema->GetAttributeSchema()->GetAttributeConfig(fieldName))
    {
        isSub = true;
        return true;
    }
    return false;
}

bool TableReaderContainerUpdater::ParseJoinRelation(OnlinePartition* mainPartition,
        const vector<JoinField>& joinFields)
{
    IndexPartitionSchemaPtr schema = mainPartition->GetSchema();
    string mainTableName = schema->GetSchemaName();
    for (size_t i = 0; i < joinFields.size(); i++)
    {
        const string &joinTableName = joinFields[i].joinTableName;
        IndexPartition* joinPartition = GetIndexPartition(joinTableName);
        if (!joinPartition)
        {
            IE_LOG(ERROR, "join table [%s] not exist",
                   joinTableName.c_str());
            return false;
        }
        const string &joinFieldName = joinFields[i].fieldName;
        bool isSub = false;
        if (!HasAttribute(mainPartition, joinFieldName, isSub))
        {
            IE_LOG(ERROR, "join field[%s] is not found in in main table [%s]",
                   joinFieldName.c_str(), mainTableName.c_str());
            if (joinFieldName.empty())
            {
                IE_LOG(ERROR, "join field empty not fail, support plugin join");
                continue;
            }
            return false;
        }
        mReverseJoinRelationMap[joinTableName][mainTableName] = joinFields[i];
        if (isSub) {
            string subMainTableName =
                schema->GetSubIndexPartitionSchema()->GetSchemaName();
            mJoinRelationMap[joinTableName][subMainTableName] = joinFields[i];
        } else {
            mJoinRelationMap[joinTableName][mainTableName] = joinFields[i];
        }

    }
    return true;
}

void TableReaderContainerUpdater::UpdateReaders()
{
    for (size_t i = 0; i < mIndexPartitions.size(); i++)
    {
        mReaderContainer->UpdateReader(
                mIndexPartitions[i]->GetSchema()->GetSchemaName(),
                mIndexPartitions[i]->GetReader());
    }
}

IE_NAMESPACE_END(partition);

