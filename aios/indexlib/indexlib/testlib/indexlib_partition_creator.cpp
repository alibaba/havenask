#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/testlib/schema_maker.h"
#include "indexlib/testlib/region_schema_maker.h"
#include "indexlib/testlib/document_creator.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/memory_control/quota_control.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(testlib);

IE_LOG_SETUP(testlib, IndexlibPartitionCreator);

IndexlibPartitionCreator::IndexlibPartitionCreator() 
{
}

IndexlibPartitionCreator::~IndexlibPartitionCreator() 
{
}

void IndexlibPartitionCreator::Init(const string& tableName,
                                    const string& fieldNames, 
                                    const string& indexNames, 
                                    const string& attributeNames, 
                                    const string& summaryNames,
                                    const string& truncateProfileStr)
{
    mSchema = CreateSchema(
            tableName, fieldNames, indexNames, attributeNames,
            summaryNames, truncateProfileStr);
}

IndexPartitionPtr IndexlibPartitionCreator::CreateIndexPartition(
        const string& rootPath, const string& docStr)
{
    return CreateIndexPartition(mSchema, rootPath, docStr);
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateSchemaWithSub(
        const IndexPartitionSchemaPtr& mainSchema,
        const IndexPartitionSchemaPtr& subSchema)
{
    IndexPartitionSchemaPtr schema = mainSchema;
    if (schema)
    {
        schema->SetSubIndexPartitionSchema(subSchema);
    }
    return schema;
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateSchema(
        const string& tableName,
        const string& fieldNames, 
        const string& indexNames, 
        const string& attributeNames, 
        const string& summaryNames,
        const string& truncateProfileStr)
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            fieldNames, indexNames, attributeNames, 
            summaryNames, truncateProfileStr);

    schema->SetSchemaName(tableName);
    return schema;
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateKKVSchema(
            const string& tableName,
            const string& fieldNames, 
            const string& pkeyField,
            const string& skeyField, 
            const string& valueNames)
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(
            fieldNames, pkeyField, skeyField, valueNames);
    schema->SetSchemaName(tableName);
    return schema;
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateMultiRegionKKVSchema(
        const string& tableName, const string& fieldNames,
        const string& regionInfos)
{
    RegionSchemaMaker maker(fieldNames, "kkv");
    vector<vector<string>> regionInfoStrVec;
    StringUtil::fromString(regionInfos, regionInfoStrVec, "#", "|");
    for (size_t i = 0; i < regionInfoStrVec.size(); i++)
    {
        if (regionInfoStrVec[i].size() == 4)
        {
            maker.AddRegion(regionInfoStrVec[i][0], regionInfoStrVec[i][1],
                            regionInfoStrVec[i][2], regionInfoStrVec[i][3],
                            "");
        }
        else if (regionInfoStrVec[i].size() == 5)
        {
            maker.AddRegion(regionInfoStrVec[i][0], regionInfoStrVec[i][1],
                            regionInfoStrVec[i][2], regionInfoStrVec[i][3],
                            regionInfoStrVec[i][4]);            
        }
        else
        {
            IE_LOG(ERROR, "region Info[%s] is invalid for kkvSchema",
                   regionInfos.c_str());
            return IndexPartitionSchemaPtr();
        }
    }
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    schema->SetSchemaName(tableName);
    return schema;
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateKVSchema(
        const string& tableName,
        const string& fieldNames, 
        const string& keyName,
        const string& valueNames,
        bool enableTTL)
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(
            fieldNames, keyName, valueNames);
    schema->SetSchemaName(tableName);
    schema->SetEnableTTL(enableTTL);
    return schema;
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateMultiRegionKVSchema(
        const string& tableName, const string& fieldNames,
        const string& regionInfos)
{
    RegionSchemaMaker maker(fieldNames, "kv");
    vector<vector<string>> regionInfoStrVec;
    StringUtil::fromString(regionInfos, regionInfoStrVec, "#", "|");
    for (size_t i = 0; i < regionInfoStrVec.size(); i++)
    {
        if (regionInfoStrVec[i].size() == 3)
        {
            maker.AddRegion(regionInfoStrVec[i][0], regionInfoStrVec[i][1],
                            regionInfoStrVec[i][2], "");
        }
        else if (regionInfoStrVec[i].size() == 4)
        {
            maker.AddRegion(regionInfoStrVec[i][0], regionInfoStrVec[i][1],
                            regionInfoStrVec[i][2], regionInfoStrVec[i][3]);            
        }
        else
        {
            IE_LOG(ERROR, "region Info[%s] is invalid for kvSchema",
                   regionInfos.c_str());
            return IndexPartitionSchemaPtr();
        }
    }
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    schema->SetSchemaName(tableName);
    return schema;
}

IndexPartitionPtr IndexlibPartitionCreator::CreateIndexPartition(
        const IndexPartitionSchemaPtr& schema,
        const string& rootPath, const string& docStr,
        const config::IndexPartitionOptions& options,
        const string& indexPluginPath,
        bool needMerge)
{
    vector<NormalDocumentPtr> docs;
    docs = DocumentCreator::CreateDocuments(schema, docStr);
    QuotaControlPtr memoryQuotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilder indexBuilder(rootPath, options, schema,
                              memoryQuotaControl, NULL, indexPluginPath);
    indexBuilder.Init();
    for (size_t i = 0; i < docs.size(); i++)
    {
        indexBuilder.Build(docs[i]);
    }
    indexBuilder.EndIndex();

    if (needMerge)
    {
        IndexPartitionOptions newOption = options;
        newOption.GetOfflineConfig().fullIndexStoreKKVTs = true;
        indexBuilder.Merge(newOption);
    }

    MemoryQuotaControllerPtr memQuotaController(new MemoryQuotaController(1024 * 1024 * 1024));
    TaskSchedulerPtr taskScheduler(new TaskScheduler());
    IndexPartitionResource partitionResource(memQuotaController, taskScheduler, NULL,
            file_system::FileBlockCachePtr(), util::SearchCachePartitionWrapperPtr(),
            indexPluginPath);
    OnlinePartitionPtr indexPartition(new OnlinePartition("", partitionResource));
    indexPartition->Open(rootPath, "", schema, options);
    return indexPartition;
}

void IndexlibPartitionCreator::AddRtDocsToPartition(
        const IndexPartitionPtr& partition,
        const std::string& docStr)
{
    vector<NormalDocumentPtr> docs;
    docs = DocumentCreator::CreateDocuments(partition->GetSchema(), docStr);
    QuotaControlPtr memoryQuotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilder indexBuilder(partition, memoryQuotaControl);
    indexBuilder.Init();
    for (size_t i = 0; i < docs.size(); i++)
    {
        indexBuilder.Build(docs[i]);
    }
}

void IndexlibPartitionCreator::BuildParallelIncData(
        const config::IndexPartitionSchemaPtr& schema,
        const std::string& rootPath,
        const index_base::ParallelBuildInfo& parallelInfo,
        const std::string& docStr,
        const config::IndexPartitionOptions& options,
        int64_t stopTs)
{
    vector<NormalDocumentPtr> docs;
    docs = DocumentCreator::CreateDocuments(schema, docStr);
    QuotaControlPtr memoryQuotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilder indexBuilder(rootPath, parallelInfo, options, schema,
                              memoryQuotaControl);
    indexBuilder.Init();
    for (size_t i = 0; i < docs.size(); i++)
    {
        indexBuilder.Build(docs[i]);
    }
    indexBuilder.EndIndex(stopTs);
}

IE_NAMESPACE_END(testlib);

