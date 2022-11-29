#include "aitheta_indexer/plugins/aitheta/test/aitheta_indexer_test.h"

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

void AithetaIndexerTest::TestPQ() {
    util::KeyValueMap params = {{DIMENSION, "72"},
                                  {INDEX_TYPE, INDEX_TYPE_PQ},
                                  {BUILD_METRIC_TYPE, L2},
                                  {SEARCH_METRIC_TYPE, L2},
                                  {USE_LINEAR_THRESHOLD, "1"},
                                  {proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM, "8192"},
                                  {proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM, "256"},
                                  {proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM, "8"},
                                  {proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA, "1000000000"},
                                  {proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO, "1"},
                                  {proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, "2000"}};
    InnerTestIndex(params);
}

void AithetaIndexerTest::TestPQWithoutCatId() {
    util::KeyValueMap params = {{DIMENSION, "72"},
                                  {INDEX_TYPE, INDEX_TYPE_PQ},
                                  {BUILD_METRIC_TYPE, L2},
                                  {SEARCH_METRIC_TYPE, L2},
                                  {USE_LINEAR_THRESHOLD, "1"},
                                  {proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM, "8192"},
                                  {proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM, "256"},
                                  {proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM, "8"},
                                  {proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA, "1000000000"},
                                  {proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO, "0.4"},
                                  {proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, "2000"}};
    InnerTestIndexWithoutCatId(params);
}

void AithetaIndexerTest::TestLinear() {
    util::KeyValueMap params = {
        {DIMENSION, "72"}, {INDEX_TYPE, INDEX_TYPE_LR}, {BUILD_METRIC_TYPE, INNER_PRODUCT}, {SEARCH_METRIC_TYPE, L2}};
    InnerTestIndex(params);
    InnerTestIndexWithoutCatId(params);
}

void AithetaIndexerTest::TestHC() {
    util::KeyValueMap params = {{DIMENSION, "72"},
                                  {INDEX_TYPE, INDEX_TYPE_HC},
                                  {BUILD_METRIC_TYPE, L2},
                                  {SEARCH_METRIC_TYPE, L2},
                                  {proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, "32"},
                                  {proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1", "2000"},
                                  {proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2", "200"},
                                  {proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, "400000"},
                                  {proxima::PARAM_HC_COMMON_LEVEL_CNT, "2"},
                                  {proxima::PARAM_HC_COMMON_MAX_DOC_CNT, "100000000"},
                                  {proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, "10000000"},
                                  {proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, "2000000000"},
                                  {PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, "0.1"}};
    InnerTestIndex(params);
}

void AithetaIndexerTest::TestHCWithoutCatId() {
    util::KeyValueMap params = {{DIMENSION, "72"},
                                  {INDEX_TYPE, INDEX_TYPE_HC},
                                  {BUILD_METRIC_TYPE, L2},
                                  {SEARCH_METRIC_TYPE, L2},
                                  {proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, "32"},
                                  {proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1", "2000"},
                                  {proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2", "200"},
                                  {proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, "400000"},
                                  {proxima::PARAM_HC_COMMON_LEVEL_CNT, "2"},
                                  {proxima::PARAM_HC_COMMON_MAX_DOC_CNT, "100000000"},
                                  {proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, "10000000"},
                                  {proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, "2000000000"},
                                  {PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, "0.1"}};
    InnerTestIndexWithoutCatId(params);
}

void AithetaIndexerTest::TestGraph() {
    util::KeyValueMap params = {{DIMENSION, "72"},
                                  {INDEX_TYPE, INDEX_TYPE_GRAPH},
                                  {BUILD_METRIC_TYPE, L2},
                                  {SEARCH_METRIC_TYPE, L2},
                                  {proxima::PARAM_GRAPH_COMMON_MAX_DOC_CNT, "100000000"},
                                  {proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw"},
                                  {proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "200"},
                                  {proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT, "200"},
                                  {proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA, "2000000000"},
                                  {proxima::PARAM_GRAPH_COMMON_WITH_QUANTIZER, "false"},
                                  {proxima::PARAM_HNSW_BUILDER_MAX_LEVEL, "10"},
                                  {proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR, "30"},
                                  {proxima::PARAM_HNSW_BUILDER_UPPER_NEIGHBOR_CNT, "10"}};
    InnerTestIndex(params);
}

void AithetaIndexerTest::TestGraphWithoutCatId() {
    util::KeyValueMap params = {{DIMENSION, "72"},
                                  {INDEX_TYPE, INDEX_TYPE_GRAPH},
                                  {BUILD_METRIC_TYPE, L2},
                                  {SEARCH_METRIC_TYPE, L2},
                                  {proxima::PARAM_GRAPH_COMMON_MAX_DOC_CNT, "100000000"},
                                  {proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw"},
                                  {proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "200"},
                                  {proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT, "200"},
                                  {proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA, "2000000000"},
                                  {proxima::PARAM_GRAPH_COMMON_WITH_QUANTIZER, "false"},
                                  {proxima::PARAM_HNSW_BUILDER_MAX_LEVEL, "10"},
                                  {proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR, "30"},
                                  {proxima::PARAM_HNSW_BUILDER_UPPER_NEIGHBOR_CNT, "10"}};
    InnerTestIndexWithoutCatId(params);
}

void AithetaIndexerTest::InnerTestIndex(const util::KeyValueMap &params) {
    IndexerPtr indexer(mFactory->createIndexer(params));
    IndexReducerPtr reducer(mFactory->createIndexReducer(params));
    IndexRetrieverPtr indexRetriever(mFactory->createIndexRetriever(params));
    IndexSegmentRetrieverPtr indexSegRetriever(mFactory->createIndexSegmentRetriever(params));

    ASSERT_TRUE(nullptr != indexer);
    ASSERT_TRUE(nullptr != reducer);
    ASSERT_TRUE(nullptr != indexRetriever);
    ASSERT_TRUE(nullptr != indexSegRetriever);

    const std::string relativeDirStr = "./aitheta_indexer/plugins/aitheta/test/";
    const std::string inputName = "8docs_72dim_multi_catid.txt";
    const std::string relativeInputPath = relativeDirStr + "/" + inputName;

    IndexerResourcePtr indexResourcePtr(new IndexerResource());
    indexResourcePtr->pluginPath = mPluginRoot;
    ASSERT_TRUE(reducer->DoInit(indexResourcePtr));

    fstream input(relativeInputPath);
    ASSERT_TRUE(input.is_open());
    autil::mem_pool::Pool pool;
    docid_t docId = 1;
    while (!input.eof()) {
        string document;
        std::getline(input, document);
        vector<string> fieldStrs(autil::StringUtil::split(document, ";"));
        ASSERT_EQ(3, fieldStrs.size());
        vector<const Field *> fields;
        IndexRawField itemId(&pool);
        itemId.SetData(autil::ConstString(fieldStrs[0], &pool));
        fields.push_back(&itemId);
        IndexRawField categoryId(&pool);
        categoryId.SetData(autil::ConstString(fieldStrs[1], &pool));
        fields.push_back(&categoryId);
        IndexRawField embedding(&pool);
        embedding.SetData(autil::ConstString(fieldStrs[2], &pool));
        fields.push_back(&embedding);

        ASSERT_TRUE(indexer->Build(fields, docId));
        ++docId;
    }
    const auto &partDir = GET_PARTITION_DIRECTORY();
    DirectoryPtr indexerDir = partDir->MakeDirectory(inputName + "_indexer");
    ASSERT_TRUE(indexer->Dump(indexerDir));

    // test reduceItem loadIndex
    IndexReduceItemPtr reduceItem(reducer->CreateReduceItem());
    ASSERT_TRUE(reduceItem->LoadIndex(indexerDir));

    docid_t baseDocId = 0;
    std::vector<docid_t> newDocIds = {0, 1, 2, -1, 3, 4, 5, 6, 7};
    // std::vector<docid_t> newDocIds = { -1, -1, -1, -1, -1, -1, -1, -1, -1 };

    DocIdMap docIdMap = CreateDocIdMap(baseDocId, newDocIds);
    ASSERT_TRUE(reduceItem->UpdateDocId(docIdMap));

    // test reducer reduce
    std::vector<IndexReduceItemPtr> reduceItems = {reduceItem};
    DirectoryPtr reducerDir = partDir->MakeDirectory(inputName + "_reducer");
    docid_t base = 0;
    SegmentMergeInfos mergeInfos({SegmentMergeInfo(0, SegmentInfo(), 0U, base)});
    mergeInfos[0].segmentInfo.docCount = 7;
    IE_NAMESPACE(index)::ReduceResource rsc(mergeInfos, ReclaimMapPtr(new ReclaimMap), true);
    OutputSegmentMergeInfo outputSegmentMergeInfo;
    outputSegmentMergeInfo.directory = reducerDir;
    ASSERT_TRUE(reducer->Reduce(reduceItems, {outputSegmentMergeInfo}, false, rsc));
    ASSERT_TRUE(indexSegRetriever->Open(reducerDir));

    // getchar();
    
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

    q = "50012772#1.384998918,-0.084735163,0.836322308,-0.185612604,-0."
        "637507260,-0.430785149,-0.301977396,-0.437412292,-0.188231066,1."
        "496632338,-0.180085152,1.471871614,-2.042002678,0.091933966,0."
        "079028606,-2.262065887,-0.188108444,0.168264002,0.000500534,-0."
        "805603683,-1.181092978,0.933989286,-0.570419371,-0.007532567,0."
        "322418988,-0.409046978,0.137126312,-0.760214329,1.383211136,-1."
        "217925549,1.329318762,-0.614403784,-0.337902784,-0.914929092,-0."
        "251350164,-1.717784882,-0.015759736,-1.341336727,-0.450480461,-1."
        "085964799,-1.032358050,-1.390653372,0.708881855,1.827503204,-0."
        "620759070,-0.481035113,-0.199365765,1.287479997,-0.474308401,-0."
        "239928782,0.985406399,-0.093536898,1.108104229,-0.322989076,-1."
        "776782393,0.472482860,-1.228474379,-0.299825817,0.199281693,-0."
        "584628940,-0.446493804,-1.549301505,2.293314695,-0.743478358,-0."
        "707596898,-0.462736905,1.047209144,-0.133819133,-0.533150673,0."
        "260926545,-0.968655765,-0.968655765";

    topK = "1";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(1, matchInfo->matchCount);
    // matchInfo->docIds[0] differ
    EXPECT_EQ(1, matchInfo->docIds[0]);
    score = 0.0F;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(0.0F, score);

    q = "50012772,50012773#1.102645874,-0.621087193,1.456478596,-0.144447133,-"
        "0.586471558,-0.072325811,-0.295576274,-0.166274160,-0.604452252,1."
        "385787964,-0.278288692,0.706716657,-0.860633492,0.037210613,0."
        "456384063,-2.329996824,0.305191100,0.047506779,0.091293998,-0."
        "506877601,-1.536866188,1.247360229,-0.362258404,-0.092898861,0."
        "426852852,-0.459051430,-0.121318057,-0.980180562,1.455726743,-1."
        "438995361,1.384310007,-0.500754893,-0.199098393,-0.929996848,-0."
        "398889095,-1.658855081,-0.048466861,-1.563427329,-0.155996293,-0."
        "761835098,-1.160272956,-1.827581525,0.772627532,1.773328304,-0."
        "397249609,-0.609549880,-0.265068829,0.946797907,-0.309519291,0."
        "100688696,0.841846347,-0.034543760,1.331876516,0.343582809,-1."
        "698662519,0.351774216,-1.535111189,-0.206797451,-0.004378542,-0."
        "327172548,-0.173534289,-0.850304663,2.410926342,-1.088050365,-0."
        "772277892,0.337354124,0.701195478,-0.678899288,0.100665532,-0."
        "519820988,-1.300492764,-1.300492764";

    topK = "3";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    matchInfo = segMatchInfos[0].matchInfo;

    EXPECT_TRUE(matchInfo->matchCount >= 3);
    EXPECT_EQ(1, matchInfo->docIds[0]);
    score = 0.0F;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(8.6832256F, score);
    EXPECT_EQ(2, matchInfo->docIds[1]);
    score = 0.0F;
    memcpy(&score, matchInfo->matchValues + 1, sizeof(score));
    EXPECT_FLOAT_EQ(0.0F, score);

    q = "50012776#1.102645874,-0.621087193,1.456478596,-0.144447133,-"
        "0.586471558,-0.072325811,-0.295576274,-0.166274160,-0.604452252,1."
        "385787964,-0.278288692,0.706716657,-0.860633492,0.037210613,0."
        "456384063,-2.329996824,0.305191100,0.047506779,0.091293998,-0."
        "506877601,-1.536866188,1.247360229,-0.362258404,-0.092898861,0."
        "426852852,-0.459051430,-0.121318057,-0.980180562,1.455726743,-1."
        "438995361,1.384310007,-0.500754893,-0.199098393,-0.929996848,-0."
        "398889095,-1.658855081,-0.048466861,-1.563427329,-0.155996293,-0."
        "761835098,-1.160272956,-1.827581525,0.772627532,1.773328304,-0."
        "397249609,-0.609549880,-0.265068829,0.946797907,-0.309519291,0."
        "100688696,0.841846347,-0.034543760,1.331876516,0.343582809,-1."
        "698662519,0.351774216,-1.535111189,-0.206797451,-0.004378542,-0."
        "327172548,-0.173534289,-0.850304663,2.410926342,-1.088050365,-0."
        "772277892,0.337354124,0.701195478,-0.678899288,0.100665532,-0."
        "519820988,-1.300492764,-1.300492764";

    topK = "1";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(0, matchInfo->matchCount);
}

void AithetaIndexerTest::InnerTestIndexWithoutCatId(const util::KeyValueMap &params) {
    IndexerPtr indexer(mFactory->createIndexer(params));
    IndexReducerPtr reducer(mFactory->createIndexReducer(params));
    IndexRetrieverPtr indexRetriever(mFactory->createIndexRetriever(params));
    IndexSegmentRetrieverPtr indexSegRetriever(mFactory->createIndexSegmentRetriever(params));

    ASSERT_TRUE(nullptr != indexer);
    ASSERT_TRUE(nullptr != reducer);
    ASSERT_TRUE(nullptr != indexRetriever);
    ASSERT_TRUE(nullptr != indexSegRetriever);

    const std::string relativeDirStr = "./aitheta_indexer/plugins/aitheta/test/";
    const std::string inputName = "8docs_72dim_no_catid.txt";
    const std::string relativeInputPath = relativeDirStr + "/" + inputName;

    IndexerResourcePtr indexResourcePtr(new IndexerResource());
    indexResourcePtr->pluginPath = mPluginRoot;
    ASSERT_TRUE(reducer->DoInit(indexResourcePtr));

    fstream input(relativeInputPath);
    ASSERT_TRUE(input.is_open());
    autil::mem_pool::Pool pool;
    docid_t docId = 1;
    while (!input.eof()) {
        string document;
        std::getline(input, document);
        vector<string> fieldStrs(autil::StringUtil::split(document, ";"));
        ASSERT_EQ(2, fieldStrs.size());
        vector<const Field *> fields;
        IndexRawField itemId(&pool);
        itemId.SetData(autil::ConstString(fieldStrs[0], &pool));
        fields.push_back(&itemId);
        IndexRawField embedding(&pool);
        embedding.SetData(autil::ConstString(fieldStrs[1], &pool));
        fields.push_back(&embedding);

        ASSERT_TRUE(indexer->Build(fields, docId));
        ++docId;
    }
    const auto &partDir = GET_PARTITION_DIRECTORY();
    DirectoryPtr indexerDir = partDir->MakeDirectory(inputName + "_indexer");
    ASSERT_TRUE(indexer->Dump(indexerDir));

    // test reduceItem loadIndex
    IndexReduceItemPtr reduceItem(reducer->CreateReduceItem());
    ASSERT_TRUE(reduceItem->LoadIndex(indexerDir));

    // test reducer reduce
    std::vector<IndexReduceItemPtr> reduceItems = {reduceItem};
    DirectoryPtr reducerDir = partDir->MakeDirectory(inputName + "_reducer");
    docid_t base = 0;
    SegmentMergeInfos mergeInfos({SegmentMergeInfo(0, SegmentInfo(), 0U, base)});
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

    q = "1.384998918,-0.084735163,0.836322308,-0.185612604,-0.637507260,-0."
        "430785149,-0.301977396,-0.437412292,-0.188231066,1.496632338,-0."
        "180085152,1.471871614,-2.042002678,0.091933966,0.079028606,-2."
        "262065887,-0.188108444,0.168264002,0.000500534,-0.805603683,-1."
        "181092978,0.933989286,-0.570419371,-0.007532567,0.322418988,-0."
        "409046978,0.137126312,-0.760214329,1.383211136,-1.217925549,1."
        "329318762,-0.614403784,-0.337902784,-0.914929092,-0.251350164,-1."
        "717784882,-0.015759736,-1.341336727,-0.450480461,-1.085964799,-1."
        "032358050,-1.390653372,0.708881855,1.827503204,-0.620759070,-0."
        "481035113,-0.199365765,1.287479997,-0.474308401,-0.239928782,0."
        "985406399,-0.093536898,1.108104229,-0.322989076,-1.776782393,0."
        "472482860,-1.228474379,-0.299825817,0.199281693,-0.584628940,-0."
        "446493804,-1.549301505,2.293314695,-0.743478358,-0.707596898,-0."
        "462736905,1.047209144,-0.133819133,-0.533150673,0.260926545,-0."
        "968655765,-0.968655765;"
        "1.331847548,0.306185782,0.761066794,0.005478872,0.563146889,-0.082780495,0.246520132,0.269936562,0.171084806,"
        "1.130794406,0.229909420,1.411579251,1.811232301,0.007333547,0.381960213,2.143728018,0.033537433,0.188311338,0."
        "049986608,0.925779462,1.145257354,0.734417796,0.562569261,0.074880809,0.479393125,0.250717789,0.180508405,0."
        "698282242,1.674801469,0.989450097,1.042539120,0.414515211,0.098752379,1.307042956,0.773260057,1.620785832,0."
        "031779364,1.513807058,0.354495317,1.042486787,1.002400041,1.472336769,0.515848637,1.942082882,0.521876693,0."
        "484890640,0.179205075,1.484310985,0.704366326,0.100792199,0.966389716,0.047377072,1.374447703,0.162600726,1."
        "737855196,0.487893552,1.528276324,0.388677686,0.043095917,0.490622401,0.471523196,1.430812359,1.943043709,0."
        "598084927,0.862527847,0.009414405,0.873509824,0.243661121,0.407721281,0.130708665,1.026132941,-0.251350164";

    topK = "1";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    matchInfo = segMatchInfos[0].matchInfo;

    EXPECT_EQ(2, matchInfo->matchCount);
    EXPECT_EQ(1, matchInfo->docIds[0]);
    score = 0.0F;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(0.0F, score);
    EXPECT_EQ(8, matchInfo->docIds[1]);

    topK = "3";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    matchInfo = segMatchInfos[0].matchInfo;

    EXPECT_EQ(6, matchInfo->matchCount);
    EXPECT_EQ(1, matchInfo->docIds[0]);
    score = 0.0F;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(0.0F, score);
    score = 0.0F;
    EXPECT_EQ(2, matchInfo->docIds[1]);
    memcpy(&score, matchInfo->matchValues + 1, sizeof(score));
    EXPECT_FLOAT_EQ(8.6832256F, score);
}

void AithetaIndexerTest::TestMinHeapUsedInReduce() {
    util::KeyValueMap params = {{DIMENSION, "8"},          {INDEX_TYPE, INDEX_TYPE_HC},
                                  {BUILD_METRIC_TYPE, L2},   {SEARCH_METRIC_TYPE, INNER_PRODUCT},
                                  {USE_DYNAMIC_PARAMS, "1"}, {USE_LINEAR_THRESHOLD, "1"}};

    IndexerPtr indexer0(mFactory->createIndexer(params));
    IndexerPtr indexer1(mFactory->createIndexer(params));
    IndexerPtr indexer2(mFactory->createIndexer(params));
    IndexReducerPtr reducer(mFactory->createIndexReducer(params));
    IndexRetrieverPtr indexRetriever(mFactory->createIndexRetriever(params));
    IndexSegmentRetrieverPtr indexSegRetriever(mFactory->createIndexSegmentRetriever(params));

    IndexerResourcePtr indexResourcePtr(new IndexerResource());
    indexResourcePtr->pluginPath = mPluginRoot;
    ASSERT_TRUE(reducer->DoInit(indexResourcePtr));

    ASSERT_TRUE(nullptr != indexer0);
    ASSERT_TRUE(nullptr != indexer1);
    ASSERT_TRUE(nullptr != indexer2);
    ASSERT_TRUE(nullptr != reducer);
    ASSERT_TRUE(nullptr != indexRetriever);
    ASSERT_TRUE(nullptr != indexSegRetriever);

    vector<vector<string>> data = {
        {"1", "4", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,1.0"}, {"2", "2", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,2.0"},
        {"3", "1", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,5.0"}, {"4", "3", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,6.0"},
        {"5", "1", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,1.0"}, {"6", "2", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,2.0"},
        {"7", "2", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,3.0"}, {"8", "1", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,4.0"},
        {"9", "1", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,5.0"}, {"10", "3", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,6.0"}};

    autil::mem_pool::Pool pool;
    docid_t docId = 0;
    for (size_t i = 0; i < data.size(); ++i) {
        vector<const Field *> fields;
        IndexRawField itemId(&pool);
        itemId.SetData(autil::ConstString(data[i][0], &pool));
        fields.push_back(&itemId);
        IndexRawField categoryId(&pool);
        categoryId.SetData(autil::ConstString(data[i][1], &pool));
        fields.push_back(&categoryId);
        IndexRawField embedding(&pool);
        embedding.SetData(autil::ConstString(data[i][2], &pool));
        fields.push_back(&embedding);
        if (docId >= 0 && docId <= 3) {
            ASSERT_TRUE(indexer0->Build(fields, docId));
        } else if (docId >= 4 && docId <= 7) {
            ASSERT_TRUE(indexer1->Build(fields, docId - 4));
        } else {
            ASSERT_TRUE(indexer2->Build(fields, docId - 8));
        }
        ++docId;
    }

    const auto &partDir = GET_PARTITION_DIRECTORY();
    DirectoryPtr indexerDir0 = partDir->MakeDirectory("./_indexer0");
    DirectoryPtr indexerDir1 = partDir->MakeDirectory("./_indexer1");
    DirectoryPtr indexerDir2 = partDir->MakeDirectory("./_indexer2");
    ASSERT_TRUE(indexer0->Dump(indexerDir0));
    ASSERT_TRUE(indexer1->Dump(indexerDir1));
    ASSERT_TRUE(indexer2->Dump(indexerDir2));

    // test reduceItem loadIndex
    IndexReduceItemPtr reduceItem0(reducer->CreateReduceItem());
    IndexReduceItemPtr reduceItem1(reducer->CreateReduceItem());
    IndexReduceItemPtr reduceItem2(reducer->CreateReduceItem());
    ASSERT_TRUE(reduceItem0->LoadIndex(indexerDir0));
    ASSERT_TRUE(reduceItem1->LoadIndex(indexerDir1));
    ASSERT_TRUE(reduceItem2->LoadIndex(indexerDir2));

    vector<docid_t> newDocIds = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    docid_t baseDocId0 = 0;
    docid_t baseDocId1 = 4;
    docid_t baseDocId2 = 8;
    DocIdMap docIdMap0 = CreateDocIdMap(baseDocId0, newDocIds);
    DocIdMap docIdMap1 = CreateDocIdMap(baseDocId1, newDocIds);
    DocIdMap docIdMap2 = CreateDocIdMap(baseDocId2, newDocIds);
    ASSERT_TRUE(reduceItem0->UpdateDocId(docIdMap0));
    ASSERT_TRUE(reduceItem1->UpdateDocId(docIdMap1));
    ASSERT_TRUE(reduceItem2->UpdateDocId(docIdMap2));

    std::vector<IndexReduceItemPtr> reduceItems = {reduceItem0, reduceItem1, reduceItem2};
    DirectoryPtr reducerDir = partDir->MakeDirectory("./_reducer");
    docid_t base = 0;
    SegmentMergeInfos mergeInfos({SegmentMergeInfo(0, SegmentInfo(), 0U, base),
                                  SegmentMergeInfo(0, SegmentInfo(), 0U, base),
                                  SegmentMergeInfo(0, SegmentInfo(), 0U, base)});
    mergeInfos[0].segmentInfo.docCount = 4;
    mergeInfos[1].segmentInfo.docCount = 4;
    mergeInfos[2].segmentInfo.docCount = 2;

    vector<DirectoryPtr> loadDirs = {indexerDir0, indexerDir1, indexerDir2};
    OutputSegmentMergeInfos outputSegMergeInfos;
    size_t size = reducer->EstimateMemoryUse(loadDirs, mergeInfos, outputSegMergeInfos, true);
    EXPECT_GE(size, 100);

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

    q = "1#1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0";

    topK = "4";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(4, matchInfo->matchCount);
    // matchInfo->docIds[0] differ
    EXPECT_EQ(2, matchInfo->docIds[0]);
    score = 0.0F;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(12.07F, score);

    EXPECT_EQ(8, matchInfo->docIds[3]);
    score = 0.0F;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(12.07F, score);

    q = "3#1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0";
    topK = "2";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(2, matchInfo->matchCount);

    EXPECT_EQ(3, matchInfo->docIds[0]);
    score = 0.0F;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(13.07F, score);

    EXPECT_EQ(9, matchInfo->docIds[1]);
    score = 0.0F;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(13.07F, score);
}

IE_NAMESPACE_END(aitheta_plugin);
