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
#include "indexlib/testlib/indexlib_partition_creator.h"

#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/region_schema_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/memory_control/QuotaControl.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::util;
using namespace indexlib::test;

namespace indexlib { namespace testlib {

IE_LOG_SETUP(testlib, IndexlibPartitionCreator);

IndexlibPartitionCreator::IndexlibPartitionCreator() {}

namespace {

vector<DocumentPtr> CreateDocuments(const IndexPartitionSchemaPtr& schema, const string& docStr)
{
    vector<DocumentPtr> docs;
    auto tableType = schema->GetTableType();
    if (tableType != tt_kv && tableType != tt_kkv) {
        auto normalDocs = DocumentCreator::CreateNormalDocuments(schema, docStr);
        for (auto doc : normalDocs) {
            docs.push_back(doc);
        }
    } else {
        auto kvDocs = DocumentCreator::CreateKVDocuments(schema, docStr);
        for (auto doc : kvDocs) {
            docs.push_back(doc);
        }
    }
    return docs;
}
} // namespace

IndexlibPartitionCreator::ImpactValueTestMode IndexlibPartitionCreator::IMPACT_VALUE_TEST_MODE =
    IndexlibPartitionCreator::IVT_DISABLE;

void IndexlibPartitionCreator::SetImpactValueTestMode(ImpactValueTestMode mode) { IMPACT_VALUE_TEST_MODE = mode; }

bool IndexlibPartitionCreator::IsImpactValueFormat()
{
    switch (IMPACT_VALUE_TEST_MODE) {
    case IVT_DISABLE:
        return false;
    case IVT_ENABLE:
        return true;
    case IVT_RANDOM: {
        return (autil::TimeUtility::currentTime() % 2 == 0);
    }
    default:
        assert(false);
        return false;
    }
    return false;
}

IndexlibPartitionCreator::~IndexlibPartitionCreator() {}

void IndexlibPartitionCreator::Init(const string& tableName, const string& fieldNames, const string& indexNames,
                                    const string& attributeNames, const string& summaryNames,
                                    const string& truncateProfileStr)
{
    mSchema = CreateSchema(tableName, fieldNames, indexNames, attributeNames, summaryNames, truncateProfileStr);
}

IndexPartitionPtr IndexlibPartitionCreator::CreateIndexPartition(const string& rootPath, const string& docStr)
{
    return CreateIndexPartition(mSchema, rootPath, docStr);
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateSchemaWithSub(const IndexPartitionSchemaPtr& mainSchema,
                                                                      const IndexPartitionSchemaPtr& subSchema)
{
    IndexPartitionSchemaPtr schema = mainSchema;
    if (schema) {
        schema->SetSubIndexPartitionSchema(subSchema);
    }
    return schema;
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateSchema(const string& tableName, const string& fieldNames,
                                                               const string& indexNames, const string& attributeNames,
                                                               const string& summaryNames,
                                                               const string& truncateProfileStr)
{
    IndexPartitionSchemaPtr schema =
        test::SchemaMaker::MakeSchema(fieldNames, indexNames, attributeNames, summaryNames, truncateProfileStr);

    schema->SetSchemaName(tableName);
    return schema;
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateKKVSchema(const string& tableName, const string& fieldNames,
                                                                  const string& pkeyField, const string& skeyField,
                                                                  const string& valueNames, bool enableTTL, int64_t ttl)
{
    string valueFormat = "";
    bool impactValue = IsImpactValueFormat();
    if (impactValue) {
        valueFormat = "impact";
    }
    IndexPartitionSchemaPtr schema =
        test::SchemaMaker::MakeKKVSchema(fieldNames, pkeyField, skeyField, valueNames, ttl, valueFormat);
    schema->SetSchemaName(tableName);
    schema->SetEnableTTL(enableTTL);
    return schema;
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateMultiRegionKKVSchema(const string& tableName,
                                                                             const string& fieldNames,
                                                                             const string& regionInfos)
{
    bool impactValue = IsImpactValueFormat();
    test::RegionSchemaMaker maker(fieldNames, "kkv");
    vector<vector<string>> regionInfoStrVec;
    StringUtil::fromString(regionInfos, regionInfoStrVec, "#", "|");
    for (size_t i = 0; i < regionInfoStrVec.size(); i++) {
        if (regionInfoStrVec[i].size() == 4) {
            maker.AddRegion(regionInfoStrVec[i][0], regionInfoStrVec[i][1], regionInfoStrVec[i][2],
                            regionInfoStrVec[i][3], "", INVALID_TTL, impactValue);
        } else if (regionInfoStrVec[i].size() == 5) {
            maker.AddRegion(regionInfoStrVec[i][0], regionInfoStrVec[i][1], regionInfoStrVec[i][2],
                            regionInfoStrVec[i][3], regionInfoStrVec[i][4], INVALID_TTL, impactValue);
        } else {
            IE_LOG(ERROR, "region Info[%s] is invalid for kkvSchema", regionInfos.c_str());
            return IndexPartitionSchemaPtr();
        }
    }
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    schema->SetSchemaName(tableName);
    return schema;
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateKVSchema(const string& tableName, const string& fieldNames,
                                                                 const string& keyName, const string& valueNames,
                                                                 bool enableTTL, int64_t ttl)
{
    string valueFormat = "";
    bool impactValue = IsImpactValueFormat();
    if (impactValue) {
        valueFormat = "impact";
    }
    IndexPartitionSchemaPtr schema;
    schema = test::SchemaMaker::MakeKVSchema(fieldNames, keyName, valueNames, ttl, valueFormat);
    schema->SetSchemaName(tableName);
    schema->SetEnableTTL(enableTTL);
    return schema;
}

IndexPartitionSchemaPtr IndexlibPartitionCreator::CreateMultiRegionKVSchema(const string& tableName,
                                                                            const string& fieldNames,
                                                                            const string& regionInfos)
{
    bool impactValue = IsImpactValueFormat();
    test::RegionSchemaMaker maker(fieldNames, "kv");
    vector<vector<string>> regionInfoStrVec;
    StringUtil::fromString(regionInfos, regionInfoStrVec, "#", "|");
    for (size_t i = 0; i < regionInfoStrVec.size(); i++) {
        if (regionInfoStrVec[i].size() == 3) {
            maker.AddRegion(regionInfoStrVec[i][0], regionInfoStrVec[i][1], regionInfoStrVec[i][2], "", INVALID_TTL,
                            impactValue);
        } else if (regionInfoStrVec[i].size() == 4) {
            maker.AddRegion(regionInfoStrVec[i][0], regionInfoStrVec[i][1], regionInfoStrVec[i][2],
                            regionInfoStrVec[i][3], INVALID_TTL, impactValue);
        } else {
            IE_LOG(ERROR, "region Info[%s] is invalid for kvSchema", regionInfos.c_str());
            return IndexPartitionSchemaPtr();
        }
    }
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    schema->SetSchemaName(tableName);
    return schema;
}

IndexPartitionPtr
IndexlibPartitionCreator::CreateIndexPartition(const IndexPartitionSchemaPtr& schema, const string& rootPath,
                                               const string& docStr, const config::IndexPartitionOptions& options,
                                               const string& indexPluginPath, bool needMerge,
                                               const indexlibv2::config::SortDescriptions& sortDescriptions)
{
    if (sortDescriptions.size() > 0) {
        index_base::PartitionMeta partitionMeta;
        for (const indexlibv2::config::SortDescription& sortDescription : sortDescriptions) {
            partitionMeta.AddSortDescription(sortDescription.GetSortFieldName(), sortDescription.GetSortPattern());
        }
        partitionMeta.Store(rootPath, nullptr);
    }

    auto docs = CreateDocuments(schema, docStr);
    QuotaControlPtr memoryQuotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilder indexBuilder(rootPath, options, schema, memoryQuotaControl, BuilderBranchHinter::Option::Test(), NULL,
                              indexPluginPath);
    if (!indexBuilder.Init()) {
        IE_LOG(ERROR, "index builder init failed");
        return nullptr;
    }
    for (size_t i = 0; i < docs.size(); i++) {
        indexBuilder.Build(docs[i]);
    }
    indexBuilder.EndIndex();
    indexBuilder.GetBranchFs()->CommitToDefaultBranch();

    if (needMerge) {
        IndexPartitionOptions newOption = options;
        newOption.GetOfflineConfig().fullIndexStoreKKVTs = true;
        indexBuilder.Merge(newOption);
    }
    indexBuilder.TEST_BranchFSMoveToMainRoad();
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController = std::make_shared<MemoryQuotaController>(1024 * 1024 * 1024);
    partitionResource.taskScheduler.reset(new TaskScheduler());
    partitionResource.indexPluginPath = indexPluginPath;
    IndexPartitionOptions newOption = options;
    setenv("TEST_QUICK_EXIT", "true", 0);
    OnlinePartitionPtr indexPartition(new OnlinePartition(partitionResource));
    indexPartition->Open(rootPath, "", schema, newOption);
    return indexPartition;
}

void IndexlibPartitionCreator::AddRtDocsToPartition(const IndexPartitionPtr& partition, const std::string& docStr)
{
    auto docs = CreateDocuments(partition->GetSchema(), docStr);
    QuotaControlPtr memoryQuotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilder indexBuilder(partition, memoryQuotaControl);
    indexBuilder.Init();
    for (size_t i = 0; i < docs.size(); i++) {
        indexBuilder.Build(docs[i]);
    }
}

void IndexlibPartitionCreator::BuildParallelIncData(const config::IndexPartitionSchemaPtr& schema,
                                                    const std::string& rootPath,
                                                    const index_base::ParallelBuildInfo& parallelInfo,
                                                    const std::string& docStr,
                                                    const config::IndexPartitionOptions& options, int64_t stopTs,
                                                    uint32_t branchId)
{
    auto docs = CreateDocuments(schema, docStr);
    uint64_t memUse = options.GetOfflineConfig().buildConfig.buildTotalMemory;
    QuotaControlPtr memoryQuotaControl(new QuotaControl(memUse * 1024 * 1024));
    // QuotaControlPtr memoryQuotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilder indexBuilder(rootPath, parallelInfo, options, schema, memoryQuotaControl,
                              BuilderBranchHinter::Option::Test(branchId));
    bool success = indexBuilder.Init();
    assert(success);
    (void)success;
    for (size_t i = 0; i < docs.size(); i++) {
        indexBuilder.Build(docs[i]);
    }
    indexBuilder.EndIndex(stopTs);
    if (branchId) {
        indexBuilder.GetBranchFs()->CommitToDefaultBranch();
    } else {
        indexBuilder.TEST_BranchFSMoveToMainRoad();
    }
}
}} // namespace indexlib::testlib
