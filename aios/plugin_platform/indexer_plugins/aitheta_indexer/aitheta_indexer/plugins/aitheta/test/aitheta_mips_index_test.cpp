#include "aitheta_indexer/plugins/aitheta/test/aitheta_mips_index_test.h"

#include <aitheta/index_distance.h>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include <fstream>
#include <indexlib/config/customized_index_config.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_module_factory.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reducer.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer_define.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/plugin/Module.h>
#include <indexlib/config/module_info.h>
#include <indexlib/test/schema_maker.h>
#include <proxima/common/params_define.h>
#include <string>

#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_retriever.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(aitheta_plugin);

void AithetaMipsIndexTest::TestLinear() {
    util::KeyValueMap params = { { DIMENSION, "8" },
                                   { INDEX_TYPE, INDEX_TYPE_LR },
                                   { BUILD_METRIC_TYPE, INNER_PRODUCT },
                                   { SEARCH_METRIC_TYPE, L2 },
                                   { MIPS_ENABLE, "true" } };
    InnerTestIndex(params);
}

void AithetaMipsIndexTest::TestQP() {
    util::KeyValueMap params = { { DIMENSION, "8" },
                                   { INDEX_TYPE, INDEX_TYPE_PQ },
                                   { BUILD_METRIC_TYPE, L2 },
                                   { SEARCH_METRIC_TYPE, INNER_PRODUCT },
                                   { USE_LINEAR_THRESHOLD, "1" },
                                   { MIPS_ENABLE, "true" },
                                   { proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM, "8192" },
                                   { proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM, "256" },
                                   { proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM, "4" },
                                   { proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA, "1000000000" },
                                   { proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO, "1" },
                                   { proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, "2000" } };
    InnerTestIndex(params);
}

void AithetaMipsIndexTest::TestHC() {
    util::KeyValueMap params = { { DIMENSION, "8" },
                                   { INDEX_TYPE, INDEX_TYPE_HC },
                                   { BUILD_METRIC_TYPE, L2 },
                                   { SEARCH_METRIC_TYPE, INNER_PRODUCT },
                                   { MIPS_ENABLE, "true" },
                                   { proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, "32" },
                                   { proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1", "2000" },
                                   { proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2", "200" },
                                   { proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, "400000" },
                                   { proxima::PARAM_HC_COMMON_LEVEL_CNT, "2" },
                                   { proxima::PARAM_HC_COMMON_MAX_DOC_CNT, "100000000" },
                                   { proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, "10000000" },
                                   { proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, "2000000000" },
                                   { PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, "0.1" } };
    InnerTestIndex(params);
}

void AithetaMipsIndexTest::TestGraph() {
    util::KeyValueMap params = { { DIMENSION, "8" },
                                   { INDEX_TYPE, INDEX_TYPE_GRAPH },
                                   { BUILD_METRIC_TYPE, L2 },
                                   { SEARCH_METRIC_TYPE, INNER_PRODUCT },
                                   { MIPS_ENABLE, "true" },
                                   { proxima::PARAM_GRAPH_COMMON_MAX_DOC_CNT, "100000000" },
                                   { proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw" },
                                   { proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "200" },
                                   { proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT, "200" },
                                   { proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA, "2000000000" },
                                   { proxima::PARAM_GRAPH_COMMON_WITH_QUANTIZER, "false" },
                                   { proxima::PARAM_HNSW_BUILDER_MAX_LEVEL, "10" },
                                   { proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR, "30" },
                                   { proxima::PARAM_HNSW_BUILDER_UPPER_NEIGHBOR_CNT, "10" } };
    InnerTestIndex(params);
}

void AithetaMipsIndexTest::InnerTestIndex(const util::KeyValueMap &params) {
    IndexerPtr indexer(mFactory->createIndexer(params));
    IndexReducerPtr reducer(mFactory->createIndexReducer(params));
    IndexRetrieverPtr indexRetriever(mFactory->createIndexRetriever(params));
    IndexSegmentRetrieverPtr indexSegRetriever(mFactory->createIndexSegmentRetriever(params));

    ASSERT_TRUE(nullptr != indexer);
    ASSERT_TRUE(nullptr != reducer);
    ASSERT_TRUE(nullptr != indexRetriever);
    ASSERT_TRUE(nullptr != indexSegRetriever);

    IndexerResourcePtr indexResourcePtr(new IndexerResource());
    indexResourcePtr->pluginPath = mPluginRoot;
    ASSERT_TRUE(reducer->DoInit(indexResourcePtr));

    vector<vector<string>> data = { { "1", "10", "1,1,1,1,1,1,1,1" }, { "2", "10", "2,2,2,2,1,1,1,1" },
                                    { "3", "20", "3,3,3,3,1,1,1,1" }, { "4", "20", "4,4,4,4,1,1,1,1" },
                                    { "5", "30", "5,5,5,5,1,1,1,1" }, { "6", "30", "6,6,6,6,1,1,1,1" },
                                    { "7", "40", "7,7,7,7,1,1,1,1" }, { "8", "50", "8,8,8,8,1,1,1,1" } };
    autil::mem_pool::Pool pool;
    docid_t docId = 1;

    for (const auto &document : data) {
        vector<const Field *> fields;
        IndexRawField id(&pool);
        id.SetData(autil::ConstString(document[0], &pool));
        fields.push_back(&id);
        IndexRawField categoryId(&pool);
        categoryId.SetData(autil::ConstString(document[1], &pool));
        fields.push_back(&categoryId);
        IndexRawField embedding(&pool);
        embedding.SetData(autil::ConstString(document[2], &pool));
        fields.push_back(&embedding);
        ASSERT_TRUE(indexer->Build(fields, docId));
        ++docId;
    }

    const auto &partDir = GET_PARTITION_DIRECTORY();
    auto method = GetValueFromKeyValueMap(params, INDEX_TYPE);
    DirectoryPtr indexerDir = partDir->MakeDirectory("/" + method + "/indexer");
    ASSERT_TRUE(indexer->Dump(indexerDir));

    // test reduceItem loadIndex
    IndexReduceItemPtr reduceItem(reducer->CreateReduceItem());
    ASSERT_TRUE(reduceItem->LoadIndex(indexerDir));

    docid_t baseDocId = 0;
    std::vector<docid_t> newDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8 };

    DocIdMap docIdMap = CreateDocIdMap(baseDocId, newDocIds);
    ASSERT_TRUE(reduceItem->UpdateDocId(docIdMap));

    // test reducer reduce
    std::vector<IndexReduceItemPtr> reduceItems = { reduceItem };
    DirectoryPtr reducerDir = partDir->MakeDirectory("/" + method + "/reducer");
    docid_t base = 0;
    SegmentMergeInfos mergeInfos({ SegmentMergeInfo(0, SegmentInfo(), 0U, base) });
    mergeInfos[0].segmentInfo.docCount = 8;
    IE_NAMESPACE(index)::ReduceResource rsc(mergeInfos, ReclaimMapPtr(new ReclaimMap), true);
    OutputSegmentMergeInfo outputSegmentMergeInfo;
    outputSegmentMergeInfo.directory = reducerDir;
    ASSERT_TRUE(reducer->Reduce(reduceItems, {outputSegmentMergeInfo}, false, rsc));
    ASSERT_TRUE(indexSegRetriever->Open(reducerDir));

    std::vector<IndexSegmentRetrieverPtr> retrievers;
    retrievers.push_back(indexSegRetriever);
    DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
    std::vector<docid_t> baseDocIds;
    baseDocIds.push_back(0);
    indexRetriever->Init(deletionMapReader, retrievers, baseDocIds);

    std::string q;
    std::string query;
    std::string topK;
    common::Term term;
    vector<SegmentMatchInfo> segMatchInfos;
    MatchInfoPtr matchInfo;
    float score = 0.0F;

    q = "10#1,1,1,1,1,1,1,1";
    topK = "1";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(1, matchInfo->matchCount);
    score = 0.0F;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    auto type = GetValueFromKeyValueMap(params, SEARCH_METRIC_TYPE);
    if (L2 == type) {
        EXPECT_EQ(1, matchInfo->docIds[0]);
        EXPECT_FLOAT_EQ(0.0F, score);
    } else if (INNER_PRODUCT == type) {
        EXPECT_EQ(2, matchInfo->docIds[0]);
        EXPECT_FLOAT_EQ(12.0F, score);
    } else {
        ASSERT_TRUE(false);
    }

    q = "10,20#0,0,0,0,1,1,1,1";
    topK = "2";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(4, matchInfo->matchCount);
    if (L2 == type) {
        EXPECT_EQ(1, matchInfo->docIds[0]);
        score = 0.0F;
        memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
        EXPECT_FLOAT_EQ(4.0F, score);

        EXPECT_EQ(4, matchInfo->docIds[3]);
        score = 0.0F;
        memcpy(&score, matchInfo->matchValues + 3, sizeof(score));
        EXPECT_FLOAT_EQ(64.0F, score);
    } else if (INNER_PRODUCT == type) {
        score = 0.0F;
        memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
        EXPECT_FLOAT_EQ(4.0F, score);
        score = 0.0F;
        memcpy(&score, matchInfo->matchValues + 3, sizeof(score));
        EXPECT_TRUE( -0.1f < (score - 4.0f) && (score - 4.0f) < 0.1f);
    } else {
        ASSERT_TRUE(false);
    }
}
IE_NAMESPACE_END(aitheta_plugin);
