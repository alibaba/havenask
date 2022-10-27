#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/partition/partition_reader_snapshot.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexApplication);

IndexApplication::IndexApplication()
{
}

IndexApplication::~IndexApplication()
{
}

bool IndexApplication::Init(const vector<IndexPartitionPtr> &indexPartitions,
                            const JoinRelationMap &joinRelations)
{
    mReaderUpdater.reset(new TableReaderContainerUpdater());
    if (!AddIndexPartitions(indexPartitions))
    {
        return false;
    }
    mReaderContainer.Init(mTableName2PartitionIdMap);
    return  mReaderUpdater->Init(mIndexPartitionVec,  joinRelations, &mReaderContainer);
}

bool IndexApplication::Init(const IndexPartitionMap &indexPartitions,
                            const JoinRelationMap &joinRelations)
{
    vector<IndexPartitionPtr> partitionVec;
    IndexPartitionMap::const_iterator iter = indexPartitions.begin();
    for (; iter != indexPartitions.end(); iter++)
    {
        partitionVec.push_back(iter->second);
    }
    return Init(partitionVec, joinRelations);
}

bool IndexApplication::Init(const vector<partition::OfflinePartitionPtr> &indexPartitions)
{
    mReaderUpdater.reset(new TableReaderContainerUpdater());
    mIndexPartitionVec.clear();
    for (size_t i = 0; i < indexPartitions.size(); i++)
    {
        mIndexPartitionVec.push_back(indexPartitions[i]);
    }
    if (!AddIndexPartitionsFromVec())
    {
        return false;
    }
    mReaderContainer.Init(mTableName2PartitionIdMap);
    for (size_t i = 0; i < indexPartitions.size(); i ++)
    {
        mReaderContainer.UpdateReader(
            mIndexPartitionVec[i]->GetSchema()->GetSchemaName(),
            mIndexPartitionVec[i]->GetReader());
    }
    return true;
}

bool IndexApplication::AddIndexPartitions(const vector<IndexPartitionPtr> &indexPartitions)
{
    mIndexPartitionVec.assign(indexPartitions.begin(), indexPartitions.end());
    return AddIndexPartitionsFromVec();
}

bool IndexApplication::AddIndexPartitionsFromVec()
{
    uint32_t id = 0;
    for (size_t i = 0; i < mIndexPartitionVec.size(); i++)
    {
        const IndexPartitionPtr &indexPart = mIndexPartitionVec[i];
        const IndexPartitionSchemaPtr &schema = indexPart->GetSchema();
        assert(schema);
        const string& tableName = schema->GetSchemaName();
        if (mTableName2PartitionIdMap.count(tableName) != 0)
        {
            IE_LOG(ERROR, "table [%s] already exist", tableName.c_str());
            return false;
        }
        mTableName2PartitionIdMap[tableName] = i;
        if (!AddIndexPartition(schema, id++))
        {
            return false;
        }
        mReaderTypeVec.push_back(READER_PRIMARY_MAIN_TYPE);
        const IndexPartitionSchemaPtr &subPartSchema =
            schema->GetSubIndexPartitionSchema();
        if (subPartSchema)
        {
            if (!AddIndexPartition(subPartSchema, id++))
            {
                return false;
            }
            mReaderTypeVec.push_back(READER_PRIMARY_SUB_TYPE);
        }
    }
    return true;
}

bool IndexApplication::AddIndexPartition(
        const IndexPartitionSchemaPtr &schema,
        uint32_t id)
{
#define add_one_schema(attrSchema) do {                                 \
        AttributeSchema::Iterator attrIter = attrSchema->Begin();       \
        for (; attrIter != attrSchema->End(); attrIter++)               \
        {                                                               \
            const AttributeConfigPtr &attrConfig = *attrIter;           \
            if (!attrConfig->IsNormal())                                \
            {                                                           \
                continue;                                               \
            }                                                           \
            const string& attrName = attrConfig->GetAttrName();         \
            if (mAttrName2IdMap.find(make_pair(tableName, attrName)) == \
                mAttrName2IdMap.end())                                  \
            {                                                           \
                mAttrName2IdMap[make_pair(tableName, attrName)] = id;   \
            }                                                           \
            else                                                        \
            {                                                           \
                IE_LOG(ERROR, "duplicate table name [%s] and attribute name[%s] " \
                       "in different table", tableName.c_str(),         \
                       attrName.c_str());                               \
                return false;                                           \
            }                                                           \
        }                                                               \
        for (size_t i = 0; i < attrSchema->GetPackAttributeCount(); ++i) \
        {                                                               \
            const PackAttributeConfigPtr &packAttrConfig = attrSchema->GetPackAttributeConfig(i); \
            const string& packName = packAttrConfig->GetAttrName();     \
            if (mPackAttrName2IdMap.find(make_pair(tableName, packName)) == \
                mPackAttrName2IdMap.end())                              \
            {                                                           \
                mPackAttrName2IdMap[make_pair(tableName, packName)] = id; \
            }                                                           \
            else                                                        \
            {                                                           \
                IE_LOG(ERROR, "duplicate table name [%s] pack attribute name[%s]" \
                       " in different table", tableName.c_str(),        \
                       packName.c_str());                               \
                return false;                                           \
            }                                                           \
        }                                                               \
    } while (0)

    const string &tableName = schema->GetSchemaName();
    const AttributeSchemaPtr &attributeSchema = schema->GetAttributeSchema();
    if (attributeSchema) {
        add_one_schema(attributeSchema);
    }
    const AttributeSchemaPtr &virtualAttrSchema = schema->GetVirtualAttributeSchema();
    if (virtualAttrSchema) {
        add_one_schema(virtualAttrSchema);
    }
    const IndexSchemaPtr &indexSchema = schema->GetIndexSchema();
    if (indexSchema)
    {
        IndexSchema::Iterator indexIter = indexSchema->Begin();
        for (; indexIter != indexSchema->End(); indexIter++)
        {
            const IndexConfigPtr &indexConfig = *indexIter;
            if (!indexConfig->IsNormal())
            {
                continue;
            }
            const string &indexName = indexConfig->GetIndexName();
            if (mIndexName2IdMap.find(make_pair(tableName, indexName)) ==
                mIndexName2IdMap.end())
            {
                mIndexName2IdMap[make_pair(tableName, indexName)] = id;
            }
            else
            {
                IE_LOG(ERROR, "duplicate table name [%s] index name[%s]"
                       " in different table", tableName.c_str(),
                       indexName.c_str());
                return false;
            }
        }
    }
    return true;
}

bool IndexApplication::IsLatestSnapshotReader(
        const PartitionReaderSnapshotPtr& snapshotReader) const
{
    assert(snapshotReader);
    uint32_t readerId = 0;
    for (uint32_t i = 0; i < mReaderContainer.Size(); i++)
    {
        const IndexPartitionReaderPtr& snapshotPartReader = 
            snapshotReader->GetIndexPartitionReader(readerId);
        if (unlikely(mReaderContainer.GetReader(i)->GetPartitionVersionHashId() !=
                     snapshotPartReader->GetPartitionVersionHashId()))
        {
            return false;
        }
        ++readerId;
        if (readerId < mReaderTypeVec.size() &&
            mReaderTypeVec[readerId] == READER_PRIMARY_SUB_TYPE)
        {
            // ignore sub reader, only check main reader
            // because sub reader has same PartitionVersion with main reader
            ++readerId;
            continue;
        }
    }
    return true;
}

bool IndexApplication::IsLatestPartitionReader(
        const IndexPartitionReaderPtr& partitionReader) const
{
    assert(partitionReader);
    const string& tableName = partitionReader->GetSchema()->GetSchemaName();
    auto iter = mTableName2PartitionIdMap.find(tableName);
    if (iter == mTableName2PartitionIdMap.end())
    {
        return false;
    }
    uint32_t id = iter->second;
    assert(id < mReaderContainer.Size());
    return mReaderContainer.GetReader(id)->GetPartitionVersionHashId() ==
        partitionReader->GetPartitionVersionHashId();
}

bool IndexApplication::UpdateSnapshotReader(
    const PartitionReaderSnapshotPtr& snapshotReader, const string& tableName) const
{
    auto iter = mTableName2PartitionIdMap.find(tableName);
    if (iter == mTableName2PartitionIdMap.end())
    {
        return false;
    }
    uint32_t id = iter->second;
    assert(id < mReaderContainer.Size());
    auto latestReader = mReaderContainer.GetReader(id);
    if(latestReader->GetPartitionVersionHashId() !=
       snapshotReader->GetIndexPartitionReader(id)->GetPartitionVersionHashId())
    {
        IndexPartitionReaderInfo partitionInfo;
        partitionInfo.indexPartReader = latestReader;
        partitionInfo.tableName = latestReader->GetSchema()->GetSchemaName();
        partitionInfo.partitionIdentifier = mIndexPartitionVec[id]->GetPartitionIdentifier();
        partitionInfo.readerType = mReaderTypeVec[id];
        snapshotReader->UpdateIndexPartitionReaderInfo(id, partitionInfo);
        return true;
    }
    return false;
}

PartitionReaderSnapshotPtr IndexApplication::CreateSnapshot()
{
    return CreateSnapshot(string());
}

PartitionReaderSnapshotPtr IndexApplication::CreateSnapshot(const std::string& leadingTableName)
{
    vector<IndexPartitionReaderInfo> indexPartReaderInfos;
    indexPartReaderInfos.resize(mReaderTypeVec.size());
    uint32_t readerId = 0;
    vector<IndexPartitionReaderPtr> snapshotIndexPartitionReaders;
    mReaderContainer.CreateSnapshot(snapshotIndexPartitionReaders);
    assert(snapshotIndexPartitionReaders.size() == mIndexPartitionVec.size());
    for (size_t i = 0; i < snapshotIndexPartitionReaders.size(); ++i)
    {
        const IndexPartitionReaderPtr &mainReader = snapshotIndexPartitionReaders[i];
        assert(mainReader);
        string tableName = mainReader->GetSchema()->GetSchemaName();
        IndexPartitionReaderInfo readerInfo;
        readerInfo.indexPartReader = mainReader;
        readerInfo.readerType = mReaderTypeVec[readerId];
        readerInfo.tableName = tableName;
        readerInfo.partitionIdentifier = mIndexPartitionVec[i]->GetPartitionIdentifier();
        indexPartReaderInfos[readerId] = readerInfo;
        ++readerId;
        if (mainReader->GetSchema()->GetTableType() == TableType::tt_customized)
        {
            continue;
        }
        const IndexPartitionReaderPtr &subReader = mainReader->GetSubPartitionReader();
        if (subReader)
        {
            IndexPartitionReaderInfo readerInfo;
            readerInfo.indexPartReader = subReader;
            readerInfo.readerType = mReaderTypeVec[readerId];
            readerInfo.tableName = tableName;//sub schema has same name with main schema
            readerInfo.partitionIdentifier = mIndexPartitionVec[i]->GetPartitionIdentifier();
            indexPartReaderInfos[readerId] = readerInfo;
            ++readerId;
        }
    }
    assert(readerId == mReaderTypeVec.size());
    return PartitionReaderSnapshotPtr(
            new PartitionReaderSnapshot(
                    &mIndexName2IdMap, 
                    &mAttrName2IdMap,
                    &mPackAttrName2IdMap,
                    indexPartReaderInfos,
                    mReaderUpdater->GetJoinRelationMap(),
                    leadingTableName));
}

void IndexApplication::GetTableSchemas(
        vector<IndexPartitionSchemaPtr>& tableSchemas)
{
    vector<IndexPartitionReaderPtr> snapshotIndexPartitionReaders;
    mReaderContainer.CreateSnapshot(snapshotIndexPartitionReaders);
    for (size_t i = 0; i < snapshotIndexPartitionReaders.size(); i++)
    {
        tableSchemas.push_back(snapshotIndexPartitionReaders[i]->GetSchema());
    }
}

IndexPartitionSchemaPtr IndexApplication::GetTableSchema(
        const string& tableName) const
{
    vector<IndexPartitionReaderPtr> snapshotIndexPartitionReaders;
    mReaderContainer.CreateSnapshot(snapshotIndexPartitionReaders);
    for (size_t i = 0; i < snapshotIndexPartitionReaders.size(); i++)
    {
        IndexPartitionSchemaPtr& schema =
            snapshotIndexPartitionReaders[i]->GetSchema();
        const string& schemaName =schema->GetSchemaName();
        if (schemaName == tableName) {
            return schema;
        }
    }
    return IndexPartitionSchemaPtr();
}

void IndexApplication::GetTableLatestDataTimestamps(
        vector<IndexDataTimestampInfo>& dataTsInfos)
{
    vector<IndexPartitionReaderPtr> snapshotIndexPartitionReaders;
    mReaderContainer.CreateSnapshot(snapshotIndexPartitionReaders);
    for (size_t i = 0; i < snapshotIndexPartitionReaders.size(); i++)
    {
        dataTsInfos.push_back(make_pair(
                        snapshotIndexPartitionReaders[i]->GetSchema()->GetSchemaName(),
                        snapshotIndexPartitionReaders[i]->GetLatestDataTimestamp()));
    }
}


bool IndexApplication::GetLatestDataTimestamp(const string& tableName, int64_t& dataTs)
{
    auto iter = mTableName2PartitionIdMap.find(tableName);
    if (iter == mTableName2PartitionIdMap.end())
    {
        return false;
    }
    uint32_t id = iter->second;
    assert(id < mReaderContainer.Size());
    dataTs = mReaderContainer.GetReader(id)->GetLatestDataTimestamp();
    return true;
}

IE_NAMESPACE_END(partition);

