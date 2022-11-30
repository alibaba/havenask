#include "aitheta_indexer/plugins/aitheta/test/aitheta_indexer_test2.h"

#include <fstream>
#include <proxima/common/params_define.h>
#include <aitheta/index_distance.h>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include <indexlib/config/customized_index_config.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer_define.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_module_factory.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reducer.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/plugin/Module.h>
#include <indexlib/config/module_info.h>
#include <indexlib/test/schema_maker.h>

#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_retriever.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(aitheta_plugin);

void AithetaIndexerTest2::TestIncBuild() {
    util::KeyValueMap params = {
        {DIMENSION, "8"}, {INDEX_TYPE, INDEX_TYPE_HC}, {BUILD_METRIC_TYPE, L2}, {SEARCH_METRIC_TYPE, L2}};

    IndexerPtr indexer(mFactory->createIndexer(params));
    ASSERT_TRUE(nullptr != indexer);

    config::IndexPartitionSchemaPtr schema;
    config::IndexPartitionOptions options;
    options.GetBuildConfig().buildPhase = BuildConfigBase::BuildPhase::BP_INC;
    util::CounterMapPtr counterMap;
    PartitionMeta pMeta;
    IndexerResourcePtr indexResource(new IndexerResource(schema, options, counterMap, pMeta, "", ""));
    ASSERT_TRUE(indexer->DoInit(indexResource));

    autil::mem_pool::Pool pool;
    {
        string pkey = "0";
        string category = "1000";
        string Embedding = "0,1,2,3,4,5,6,7";
        IndexRawField pkeyField(&pool);
        IndexRawField categoryField(&pool);
        IndexRawField EmbeddingField(&pool);
        pkeyField.SetData(ConstString(pkey, &pool));
        categoryField.SetData(ConstString(category, &pool));
        EmbeddingField.SetData(ConstString(Embedding, &pool));
        vector<const Field*> fields = {&pkeyField, &categoryField, &EmbeddingField};
        ASSERT_TRUE(indexer->Build(fields, 0));
    }
    {
        string pkey = "1";
        string category;
        string Embedding;
        IndexRawField pkeyField(&pool);
        IndexRawField categoryField(&pool);
        IndexRawField EmbeddingField(&pool);
        pkeyField.SetData(ConstString(pkey, &pool));
        categoryField.SetData(ConstString(category, &pool));
        EmbeddingField.SetData(ConstString(Embedding, &pool));
        vector<const Field*> fields = {&pkeyField, &categoryField, &EmbeddingField};
        ASSERT_TRUE(indexer->Build(fields, 1));
    }

    auto segment = DYNAMIC_POINTER_CAST(AithetaIndexer, indexer)->mBuilder->mRawSegment;
    auto catId2Records = segment->GetCatId2Records();
    ASSERT_EQ(1, catId2Records.size());

    const auto& records = catId2Records[INVALID_CATEGORY_ID];
    ASSERT_EQ(0, get<0>(records)[0]);
    ASSERT_EQ(0, get<1>(records)[0]);

    ASSERT_EQ(1, get<0>(records)[1]);
    ASSERT_EQ(1, get<1>(records)[1]);

    ASSERT_EQ(0u, get<2>(records).size());

    const auto& rootDir = GET_PARTITION_DIRECTORY();
    DirectoryPtr dumpDir = rootDir->MakeDirectory("testIncBuild");
    ASSERT_TRUE(indexer->Dump(dumpDir));

    IndexReducerPtr reducer(mFactory->createIndexReducer(params));
    ASSERT_TRUE(nullptr != reducer);
    IndexReduceItemPtr reduceItem(reducer->CreateReduceItem());
    ASSERT_TRUE(reduceItem->LoadIndex(dumpDir));
}

void AithetaIndexerTest2::TestBuildWithUnexpectedField() {
    util::KeyValueMap params = {{DIMENSION, "8"},          {INDEX_TYPE, INDEX_TYPE_HC},
                                {BUILD_METRIC_TYPE, L2},   {SEARCH_METRIC_TYPE, INNER_PRODUCT},
                                {USE_DYNAMIC_PARAMS, "1"}, {USE_LINEAR_THRESHOLD, "1"}};

    {
        // Inc Build With One Field Success
        IndexerPtr indexer(mFactory->createIndexer(params));

        config::IndexPartitionSchemaPtr schema;
        config::IndexPartitionOptions options;
        options.GetBuildConfig().buildPhase = BuildConfigBase::BuildPhase::BP_INC;
        util::CounterMapPtr counterMap;
        PartitionMeta pMeta;
        IndexerResourcePtr indexResource(new IndexerResource(schema, options, counterMap, pMeta, "", ""));
        ASSERT_TRUE(indexer->DoInit(indexResource));

        ASSERT_TRUE(nullptr != indexer);

        autil::mem_pool::Pool pool;
        docid_t docId = 0;
        vector<const Field*> fields;
        IndexRawField pkey(&pool);
        pkey.SetData(autil::ConstString("1", &pool));
        fields.push_back(&pkey);

        ASSERT_TRUE(indexer->Build(fields, 0));
        DirectoryPtr indexerDir = GET_PARTITION_DIRECTORY()->MakeDirectory("TestBuildWithUnexpectedField_1");
        ASSERT_TRUE(indexer->Dump(indexerDir));
    }
    {
        // Full Build With One Field Failure
        IndexerPtr indexer(mFactory->createIndexer(params));

        config::IndexPartitionSchemaPtr schema;
        config::IndexPartitionOptions options;
        options.GetBuildConfig().buildPhase = BuildConfigBase::BuildPhase::BP_FULL;
        util::CounterMapPtr counterMap;
        PartitionMeta pMeta;
        IndexerResourcePtr indexResource(new IndexerResource(schema, options, counterMap, pMeta, "", ""));
        ASSERT_TRUE(indexer->DoInit(indexResource));

        ASSERT_TRUE(nullptr != indexer);

        autil::mem_pool::Pool pool;
        docid_t docId = 0;
        vector<const Field*> fields;
        IndexRawField pkey(&pool);
        pkey.SetData(autil::ConstString("1", &pool));
        fields.push_back(&pkey);

        ASSERT_FALSE(indexer->Build(fields, 0));
        DirectoryPtr indexerDir = GET_PARTITION_DIRECTORY()->MakeDirectory("TestBuildWithUnexpectedField_2");
        ASSERT_TRUE(indexer->Dump(indexerDir));
    }

    {
        // Full Build With Two Field Failure
        IndexerPtr indexer(mFactory->createIndexer(params));

        config::IndexPartitionSchemaPtr schema;
        config::IndexPartitionOptions options;
        options.GetBuildConfig().buildPhase = BuildConfigBase::BuildPhase::BP_FULL;
        util::CounterMapPtr counterMap;
        PartitionMeta pMeta;
        IndexerResourcePtr indexResource(new IndexerResource(schema, options, counterMap, pMeta, "", ""));
        ASSERT_TRUE(indexer->DoInit(indexResource));

        ASSERT_TRUE(nullptr != indexer);

        autil::mem_pool::Pool pool;
        docid_t docId = 0;
        vector<const Field*> fields;
        IndexRawField pkey(&pool);
        pkey.SetData(autil::ConstString("1", &pool));
        fields.push_back(&pkey);
        IndexRawField category(&pool);
        category.SetData(autil::ConstString("50010850", &pool));
        fields.push_back(&category);
        ASSERT_FALSE(indexer->Build(fields, 0));
        DirectoryPtr indexerDir = GET_PARTITION_DIRECTORY()->MakeDirectory("TestBuildWithUnexpectedField_3");
        ASSERT_TRUE(indexer->Dump(indexerDir));
    }
    {
        // Inc Build With Two Field Success
        IndexerPtr indexer(mFactory->createIndexer(params));

        config::IndexPartitionSchemaPtr schema;
        config::IndexPartitionOptions options;
        options.GetBuildConfig().buildPhase = BuildConfigBase::BuildPhase::BP_INC;
        util::CounterMapPtr counterMap;
        PartitionMeta pMeta;
        IndexerResourcePtr indexResource(new IndexerResource(schema, options, counterMap, pMeta, "", ""));
        ASSERT_TRUE(indexer->DoInit(indexResource));

        ASSERT_TRUE(nullptr != indexer);

        autil::mem_pool::Pool pool;
        docid_t docId = 0;
        vector<const Field*> fields;
        IndexRawField pkey(&pool);
        pkey.SetData(autil::ConstString("1", &pool));
        fields.push_back(&pkey);
        IndexRawField category(&pool);
        category.SetData(autil::ConstString("50010850", &pool));
        fields.push_back(&category);
        ASSERT_TRUE(indexer->Build(fields, 0));
        DirectoryPtr indexerDir = GET_PARTITION_DIRECTORY()->MakeDirectory("TestBuildWithUnexpectedField_4");
        ASSERT_TRUE(indexer->Dump(indexerDir));
    }
    {
        // Full Build With Two Field Success
        IndexerPtr indexer(mFactory->createIndexer(params));

        config::IndexPartitionSchemaPtr schema;
        config::IndexPartitionOptions options;
        options.GetBuildConfig().buildPhase = BuildConfigBase::BuildPhase::BP_FULL;
        util::CounterMapPtr counterMap;
        PartitionMeta pMeta;
        IndexerResourcePtr indexResource(new IndexerResource(schema, options, counterMap, pMeta, "", ""));
        ASSERT_TRUE(indexer->DoInit(indexResource));

        ASSERT_TRUE(nullptr != indexer);

        autil::mem_pool::Pool pool;
        docid_t docId = 0;
        vector<const Field*> fields;
        IndexRawField pkey(&pool);
        pkey.SetData(autil::ConstString("1", &pool));
        fields.push_back(&pkey);
        IndexRawField embedding(&pool);
        embedding.SetData(autil::ConstString("1,2,3,4,5,6,7,8", &pool));
        fields.push_back(&embedding);
        ASSERT_TRUE(indexer->Build(fields, 0));
        DirectoryPtr indexerDir = GET_PARTITION_DIRECTORY()->MakeDirectory("TestBuildWithUnexpectedField_5");
        ASSERT_TRUE(indexer->Dump(indexerDir));
    }
    {
        // Full Build With Three Field Success
        IndexerPtr indexer(mFactory->createIndexer(params));

        config::IndexPartitionSchemaPtr schema;
        config::IndexPartitionOptions options;
        options.GetBuildConfig().buildPhase = BuildConfigBase::BuildPhase::BP_FULL;
        util::CounterMapPtr counterMap;
        PartitionMeta pMeta;
        IndexerResourcePtr indexResource(new IndexerResource(schema, options, counterMap, pMeta, "", ""));
        ASSERT_TRUE(indexer->DoInit(indexResource));

        ASSERT_TRUE(nullptr != indexer);

        autil::mem_pool::Pool pool;
        docid_t docId = 0;
        vector<const Field*> fields;
        IndexRawField pkey(&pool);
        pkey.SetData(autil::ConstString("1", &pool));
        fields.push_back(&pkey);
        IndexRawField category(&pool);
        category.SetData(autil::ConstString("50010580", &pool));
        fields.push_back(&category);
        IndexRawField embedding(&pool);
        embedding.SetData(autil::ConstString("1,2,3,4,5,6,7,8", &pool));
        fields.push_back(&embedding);
        ASSERT_TRUE(indexer->Build(fields, 0));
        DirectoryPtr indexerDir = GET_PARTITION_DIRECTORY()->MakeDirectory("TestBuildWithUnexpectedField_6");
        ASSERT_TRUE(indexer->Dump(indexerDir));
    }
}

IE_NAMESPACE_END(aitheta_plugin);
