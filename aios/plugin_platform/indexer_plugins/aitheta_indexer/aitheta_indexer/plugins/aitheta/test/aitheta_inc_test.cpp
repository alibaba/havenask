#include "aitheta_indexer/plugins/aitheta/test/aitheta_inc_test.h"

#include <fstream>
#include <proxima/common/params_define.h>

#include <aitheta/index_distance.h>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include <indexlib/config/customized_index_config.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_module_factory.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reducer.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/plugin/Module.h>
#include <indexlib/config/module_info.h>
#include <indexlib/storage/file_system_wrapper.h>
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
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(aitheta_plugin);

void AithetaIncTest::InnerTestInc(const util::KeyValueMap &params) {
    const auto &partitionDir = GET_PARTITION_DIRECTORY();
    // raw to index
    DirectoryPtr indexSegDir = partitionDir->MakeDirectory("InnerTestInc");
    {
        vector<docid_t> docIds = {0, 1, 2, 3, 4, 5, 6, 7};
        IndexReduceItemPtr reduceItem;

        docid_t begin = 0;
        const string srcs = "8docs_72dim_single_catid.txt";
        DocIdMap docIdMap = CreateDocIdMap(begin, docIds);
        auto loadDir = CreateReduceItem(params, srcs, begin, docIdMap, reduceItem);
        IndexReducerPtr reducer(mFactory->createIndexReducer(params));
        ASSERT_TRUE(nullptr != reducer);

        IndexerResourcePtr indexResourcePtr(new IndexerResource());
        indexResourcePtr->pluginPath = mPluginRoot;
        ASSERT_TRUE(reducer->DoInit(indexResourcePtr));

        vector<DirectoryPtr> loadDirs = {loadDir};
        docid_t base = 0;
        SegmentMergeInfos mergeInfos({SegmentMergeInfo(0, SegmentInfo(), 0U, base)});
        mergeInfos[0].segmentInfo.docCount = 8;
        OutputSegmentMergeInfos outputSegMergeInfos;
        size_t size = reducer->EstimateMemoryUse(loadDirs, mergeInfos, outputSegMergeInfos, true);
        EXPECT_GE(size, 100);

        IE_NAMESPACE(index)::ReduceResource rsc(mergeInfos, ReclaimMapPtr(new ReclaimMap), true);
        std::vector<IndexReduceItemPtr> reduceItems = {reduceItem};

        OutputSegmentMergeInfo outputSegmentMergeInfo;
        outputSegmentMergeInfo.directory = indexSegDir;
        
        ASSERT_TRUE(reducer->Reduce(reduceItems, {outputSegmentMergeInfo}, false, rsc));
        IndexSegmentRetrieverPtr indexSegRetriever(mFactory->createIndexSegmentRetriever(params));
        ASSERT_TRUE(nullptr != indexSegRetriever);
        EXPECT_EQ(InnerGetIndexSegmentLoad4RetrieveMemoryUse(indexSegDir->GetPath()),
                  indexSegRetriever->EstimateLoadMemoryUse(indexSegDir));
    }

    // raw to raw
    DirectoryPtr rawSegDir = partitionDir->MakeDirectory("raw_segment_reduce");
    DirectoryPtr loadDir0;
    DirectoryPtr loadDir1;
    {
        vector<docid_t> docIds = {-1, 0, -1, -1, 1, 2};
        vector<DirectoryPtr> loadDirs;

        IndexReduceItemPtr reduceItem0;
        {
            docid_t begin = 0;
            const string srcs = "3docs_72dim_single_catid_inc_1.txt";
            DocIdMap docIdMap = CreateDocIdMap(begin, docIds);
            loadDir0 = CreateReduceItem(params, srcs, begin, docIdMap, reduceItem0);
            loadDirs.push_back(loadDir0);
        }

        IndexReduceItemPtr reduceItem1;
        {
            docid_t baseDocId = 3;
            const string srcs = "3docs_72dim_single_catid_inc_2.txt";
            DocIdMap docIdMap = CreateDocIdMap(baseDocId, docIds);
            loadDir1 = CreateReduceItem(params, srcs, 0, docIdMap, reduceItem1);
            loadDirs.push_back(loadDir1);
        }

        docid_t base = 8;
        SegmentMergeInfos mergeInfos(2, SegmentMergeInfo(0, SegmentInfo(), 0U, base));
        mergeInfos[0].segmentInfo.docCount = 1;
        mergeInfos[1].segmentInfo.docCount = 2;
        IE_NAMESPACE(index)::ReduceResource rsc(mergeInfos, ReclaimMapPtr(new ReclaimMap), false);

        IndexReducerPtr reducer(mFactory->createIndexReducer(params));
        ASSERT_TRUE(nullptr != reducer);

        IndexerResourcePtr indexResourcePtr(new IndexerResource());
        indexResourcePtr->pluginPath = mPluginRoot;
        ASSERT_TRUE(reducer->DoInit(indexResourcePtr));
        OutputSegmentMergeInfos outputSegMergeInfos;
        size_t size = reducer->EstimateMemoryUse(loadDirs, mergeInfos, outputSegMergeInfos, true);
        EXPECT_GE(size, 100);

        // enable raw segment reduce
        std::vector<IndexReduceItemPtr> reduceItems = {reduceItem0, reduceItem1};
        OutputSegmentMergeInfo outputSegMergeInfo;
        outputSegMergeInfo.directory = rawSegDir;
        ASSERT_TRUE(reducer->Reduce(reduceItems, {outputSegMergeInfo}, false, rsc));
        auto reduceItem = reducer->CreateReduceItem();
        ASSERT_TRUE(reduceItem->LoadIndex(rawSegDir));
        docid_t baseDocId = 0;
        docIds = {8, 9, 10};
        DocIdMap docIdMap = CreateDocIdMap(baseDocId, docIds);
        ASSERT_TRUE(reduceItem->UpdateDocId(docIdMap));
        auto &segment = DYNAMIC_POINTER_CAST(AithetaIndexReduceItem, reduceItem)->mSegment;
        auto rawSegment = DYNAMIC_POINTER_CAST(RawSegment, segment);
        ASSERT_TRUE(rawSegment);
        auto &catId2DocIds = rawSegment->GetCatId2Records();
        EXPECT_EQ(2U, catId2DocIds.size());
        auto it = catId2DocIds.begin();
        EXPECT_EQ(50012772LL, it->first);
        const auto &firstDocIds = get<1>(it->second);
        const auto &firstPkeys = get<0>(it->second);
        EXPECT_EQ(2U, firstDocIds.size());
        EXPECT_EQ(9, firstDocIds[0]);
        EXPECT_EQ(10000000000000, firstPkeys[0]);
        EXPECT_EQ(8, firstDocIds[1]);
        EXPECT_EQ(20000000000000, firstPkeys[1]);
        ++it;
        EXPECT_EQ(50012774LL, it->first);
        const auto &secondDocIds = get<1>(it->second);
        const auto &secondPkeys = get<0>(it->second);
        EXPECT_EQ(1U, secondDocIds.size());
        EXPECT_EQ(-1, secondDocIds[0]);
        EXPECT_EQ(40000000000000, secondPkeys[0]);
    }

    // mix to index
    DirectoryPtr mixDir = partitionDir->MakeDirectory("mix_reduce");
    {
        auto reducer = mFactory->createIndexReducer(params);
        ASSERT_TRUE(nullptr != reducer);
        IndexerResourcePtr indexResourcePtr(new IndexerResource());
        indexResourcePtr->pluginPath = mPluginRoot;
        ASSERT_TRUE(reducer->DoInit(indexResourcePtr));

        vector<docid_t> docIds = {-1, -1, 0, -1, 1, 2, 3, 4, 5, 6, 7};
        auto indexSegReduceItem = reducer->CreateReduceItem();
        {
            ASSERT_TRUE(indexSegReduceItem->LoadIndex(indexSegDir));
            // old docId : {0, 1, 6, 7, 2, 5, 3, 4}
            // pkey: {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000}
            // new docId : {-1, -1, 3, 4, 0, 2, -1, 1}
            docid_t baseDocId = 0;
            DocIdMap docIdMap = CreateDocIdMap(baseDocId, docIds);

            auto &segment = DYNAMIC_POINTER_CAST(AithetaIndexReduceItem, indexSegReduceItem)->mSegment;
            auto indexSegment = DYNAMIC_POINTER_CAST(IndexSegment, segment);
            const auto &docIdArray = indexSegment->getDocIdArray();
            ASSERT_TRUE(indexSegReduceItem->UpdateDocId(docIdMap));
        }
        auto rawSegReduceItem = reducer->CreateReduceItem();
        {
            ASSERT_TRUE(rawSegReduceItem->LoadIndex(rawSegDir));
            // old docId :{0, 1, -1}
            // pkey: {2000, 1000, 4000}
            // new docId :{5, 6, -1}
            docid_t baseDocId = 8;
            DocIdMap docIdMap =  CreateDocIdMap(baseDocId, docIds);
            ASSERT_TRUE(rawSegReduceItem->UpdateDocId(docIdMap));
        }

        vector<IndexReduceItemPtr> reduceItems = {indexSegReduceItem, rawSegReduceItem};
        vector<DirectoryPtr> loadDirs = {indexSegDir, rawSegDir};

        docid_t base = 0;
        SegmentMergeInfos mergeInfos(
            {SegmentMergeInfo(0, SegmentInfo(), 0U, base), SegmentMergeInfo(0, SegmentInfo(), 0U, base)});
        mergeInfos[0].segmentInfo.docCount = 5;
        mergeInfos[1].segmentInfo.docCount = 3;
        IE_NAMESPACE(index)::ReduceResource rsc(mergeInfos, ReclaimMapPtr(new ReclaimMap), true);
        OutputSegmentMergeInfos outputSegMergeInfos;
        size_t size = reducer->EstimateMemoryUse(loadDirs, mergeInfos, outputSegMergeInfos, true);
        EXPECT_GE(size, 100);

        OutputSegmentMergeInfo outputSegmentMergeInfo;
        outputSegmentMergeInfo.directory = mixDir;
        ASSERT_TRUE(reducer->Reduce(reduceItems, {outputSegmentMergeInfo}, true, rsc));
        // produced segment used in the next merge
        {
            // {-1, -1, 3, 4, 0, 2, -1, 1} ->   {6, 5, 3, 4, 0, 2, -1, 1}
            auto reduceItem = reducer->CreateReduceItem();
            ASSERT_TRUE(reduceItem->LoadIndex(mixDir));
            docid_t baseDocId = 0;
            vector<docid_t> docIds = {0, 1, 2, 3, 4, 5, 6, 7};
            DocIdMap docIdMap = CreateDocIdMap(baseDocId, docIds);
            auto &segment = DYNAMIC_POINTER_CAST(AithetaIndexReduceItem, reduceItem)->mSegment;
            auto indexSegment = DYNAMIC_POINTER_CAST(IndexSegment, segment);
            const auto &docIdArray = indexSegment->getDocIdArray();
            EXPECT_EQ(8U, docIdArray.size());
            for (auto id : docIdArray) {
                cout << id << endl;
            }
            cout << endl;
            ASSERT_TRUE(reduceItem->UpdateDocId(docIdMap));
            ASSERT_TRUE(indexSegment);
            EXPECT_EQ(8U, docIdArray.size());
            for (auto id : docIdArray) {
                cout << id << endl;
            }
            EXPECT_EQ(6, docIdArray[0]);
            EXPECT_EQ(5, docIdArray[1]);
            EXPECT_EQ(4, docIdArray[3]);
            EXPECT_EQ(1, docIdArray[7]);
        }
        // produced segment used in the retriever
        {
            IndexRetrieverPtr indexRetriever(mFactory->createIndexRetriever(params));
            IndexSegmentRetrieverPtr indexSegRetriever(mFactory->createIndexSegmentRetriever(params));
            ASSERT_TRUE(nullptr != indexRetriever);
            ASSERT_TRUE(nullptr != indexSegRetriever);

            EXPECT_EQ(InnerGetIndexSegmentLoad4RetrieveMemoryUse(indexSegDir->GetPath()),
                      indexSegRetriever->EstimateLoadMemoryUse(indexSegDir));
            ASSERT_TRUE(indexSegRetriever->Open(indexSegDir));

            std::vector<IndexSegmentRetrieverPtr> segRetrievers{indexSegRetriever};
            DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
            std::vector<docid_t> baseDocIds{0};
            ASSERT_TRUE(indexRetriever->Init(deletionMapReader, segRetrievers, baseDocIds));
        }
        {
            IndexRetrieverPtr indexRetriever(mFactory->createIndexRetriever(params));
            IndexSegmentRetrieverPtr indexSegRetriever(mFactory->createIndexSegmentRetriever(params));
            ASSERT_TRUE(nullptr != indexRetriever);
            ASSERT_TRUE(nullptr != indexSegRetriever);

            EXPECT_EQ(InnerGetIndexSegmentLoad4RetrieveMemoryUse(mixDir->GetPath()),
                      indexSegRetriever->EstimateLoadMemoryUse(mixDir));

            ASSERT_TRUE(indexSegRetriever->Open(mixDir));

            std::vector<IndexSegmentRetrieverPtr> segRetrievers{indexSegRetriever};
            DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
            std::vector<docid_t> baseDocIds{0};
            ASSERT_TRUE(indexRetriever->Init(deletionMapReader, segRetrievers, baseDocIds));
        }
    }
    // mix load
    {
        IndexRetrieverPtr indexRetriever(mFactory->createIndexRetriever(params));
        IndexSegmentRetrieverPtr rawSegRetriever0(mFactory->createIndexSegmentRetriever(params));
        IndexSegmentRetrieverPtr rawSegRetriever1(mFactory->createIndexSegmentRetriever(params));
        IndexSegmentRetrieverPtr indexSegRetriever(mFactory->createIndexSegmentRetriever(params));
        ASSERT_TRUE(nullptr != indexRetriever);
        ASSERT_TRUE(nullptr != rawSegRetriever0);
        ASSERT_TRUE(nullptr != rawSegRetriever1);
        ASSERT_TRUE(nullptr != indexSegRetriever);

        // segment docId: { 4, 5, 6, 7 }
        EXPECT_GE(rawSegRetriever0->EstimateLoadMemoryUse(loadDir0), 10);
        EXPECT_GE(rawSegRetriever1->EstimateLoadMemoryUse(loadDir1), 10);
        EXPECT_EQ(InnerGetIndexSegmentLoad4RetrieveMemoryUse(indexSegDir->GetPath()),
                  indexSegRetriever->EstimateLoadMemoryUse(indexSegDir));

        ASSERT_TRUE(rawSegRetriever0->Open(loadDir0));
        ASSERT_TRUE(rawSegRetriever1->Open(loadDir1));
        // segment docId: { 0, 1, 2, 3, 4, 5, 6, 7 }
        ASSERT_TRUE(indexSegRetriever->Open(indexSegDir));

        std::vector<IndexSegmentRetrieverPtr> segRetrievers{indexSegRetriever, rawSegRetriever0, rawSegRetriever1};
        DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
        std::vector<docid_t> baseDocIds{0, 8, 11};
        // { 0,1,2,3,4,5,6,7}->{104,105,106,107,4,5,6,7}
        // { 0,1,2,3,4,5,6,7}->{12,9,2,13,4,5,6,7}
        ASSERT_TRUE(indexRetriever->Init(deletionMapReader, segRetrievers, baseDocIds));

        autil::mem_pool::Pool pool;
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

        topK = "2";
        query = q + "&n=" + topK;
        term.SetWord(query);
        segMatchInfos = indexRetriever->Search(term, &pool);
        matchInfo = segMatchInfos[0].matchInfo;
        EXPECT_EQ(2, matchInfo->matchCount);
        EXPECT_EQ(9, matchInfo->docIds[0]);
        EXPECT_EQ(12, matchInfo->docIds[1]);

        score = 0.0F;
        memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
        EXPECT_FLOAT_EQ(59.37606F, score);
        memcpy(&score, matchInfo->matchValues + 1, sizeof(score));
        EXPECT_FLOAT_EQ(63.752316F, score);

        q = "50012773#0.294925421,0.048132315,0.983033299,-0.404720277,-0."
            "601902485,-0.811883152,-0.040542543,-0.345674545,-0.598592103,1."
            "997333884,-0.495106876,0.471447289,-1.905154467,-0.244898498,0."
            "300568551,-1.553430915,-1.165269494,0.202129364,-0.189726382,1."
            "088752627,-1.389436245,0.996046126,-0.561909556,-0.106272012,0."
            "266557574,0.045689493,0.771393776,0.344331741,-0.198470995,-0."
            "777254760,0.572947979,-1.018496513,-0.590095282,-1.082069874,-0."
            "193634689,-0.899429321,0.052893013,-0.197206110,-0.393786311,-1."
            "487919092,-0.965944886,-0.203329936,0.695428669,1.983488560,-0."
            "390734702,-1.013209343,-0.925248623,2.556200027,0.138484836,-0."
            "685090661,0.647506118,0.913659096,0.478595525,-0.453289211,-1."
            "845363259,-0.177751750,0.611178935,0.062349375,0.345484316,-1."
            "154664040,-1.239553452,-0.977678418,1.095051527,0.226935357,0."
            "110947505,-1.488319635,-0.227789372,-0.062516183,-0.458227575,1."
            "167940855,-0.701067567,-0.701067567";

        topK = "3";
        query = q + "&n=" + topK;
        term.SetWord(query);
        segMatchInfos = indexRetriever->Search(term, &pool);
        matchInfo = segMatchInfos[0].matchInfo;

        EXPECT_EQ(2, matchInfo->matchCount);

        EXPECT_EQ(2, matchInfo->docIds[0]);
        EXPECT_EQ(5, matchInfo->docIds[1]);

        score = 0.0F;
        memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
        EXPECT_FLOAT_EQ(56.801079F, score);
        score = 0.0F;
        memcpy(&score, matchInfo->matchValues + 1, sizeof(score));
        EXPECT_FLOAT_EQ(-2.8069935F, score);

        q = "50012773,50012774#0.015759736,-1.341336727,-0.450480461,-1."
            "085964799,-1.032358050,-1.390653372,0.708881855,1.827503204,-0."
            "620759070,-0.481035113,-0.199365765,1.287479997,-0.474308401,-0."
            "239928782,0.985406399,-0.093536898,1.108104229,-0.322989076,-1."
            "776782393,0.472482860,-1.228474379,-0.299825817,0.199281693,-0."
            "584628940,-0.446493804,-1.549301505,2.293314695,-0.743478358,-0."
            "707596898,-0.462736905,1.047209144,-0.133819133,-0.533150673,0."
            "260926545,-0.968655765,-0.347293258,1.384998918,-0.084735163,0."
            "836322308,-0.185612604,-0.637507260,-0.430785149,-0.301977396,-0."
            "437412292,-0.188231066,1.496632338,-0.180085152,1.471871614,-2."
            "042002678,0.091933966,0.079028606,-2.262065887,-0.188108444,0."
            "168264002,0.000500534,-0.805603683,-1.181092978,0.933989286,-0."
            "570419371,-0.007532567,0.322418988,-0.409046978,0.137126312,-0."
            "760214329,1.383211136,-1.217925549,1.329318762,-0.614403784,-0."
            "337902784,-0.914929092,-0.251350164,-0.251350164";

        topK = "2";
        query = q + "&n=" + topK;
        term.SetWord(query);
        segMatchInfos = indexRetriever->Search(term, &pool);
        matchInfo = segMatchInfos[0].matchInfo;

        EXPECT_EQ(4, matchInfo->matchCount);
        EXPECT_EQ(2, matchInfo->docIds[0]);
        EXPECT_EQ(4, matchInfo->docIds[1]);
        EXPECT_EQ(5, matchInfo->docIds[2]);
        EXPECT_EQ(13, matchInfo->docIds[3]);

        score = 0.0F;
        memcpy(&score, matchInfo->matchValues + 1, sizeof(score));
        EXPECT_FLOAT_EQ(60.047028F, score);
        score = 0.0F;
        memcpy(&score, matchInfo->matchValues + 3, sizeof(score));
        EXPECT_FLOAT_EQ(15.096784F, score);
    }
}

void AithetaIncTest::TestPQInc() {
    util::KeyValueMap params = {{DIMENSION, "72"},
                                  {INDEX_TYPE, INDEX_TYPE_PQ},
                                  {BUILD_METRIC_TYPE, L2},
                                  {SEARCH_METRIC_TYPE, INNER_PRODUCT},
                                  {USE_LINEAR_THRESHOLD, "1"},
                                  {proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM, "8192"},
                                  {proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM, "256"},
                                  {proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM, "8"},
                                  {proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA, "1000000000"},
                                  {proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO, "1"},
                                  {proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, "2000"}};

    InnerTestInc(params);
}

void AithetaIncTest::TestHCInc() {
    const string kLevel1 = proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1";
    const string kLevel2 = proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2";

    util::KeyValueMap params = {{DIMENSION, "72"},
                                  {INDEX_TYPE, INDEX_TYPE_HC},
                                  {BUILD_METRIC_TYPE, L2},
                                  {SEARCH_METRIC_TYPE, INNER_PRODUCT},
                                  {USE_LINEAR_THRESHOLD, "1"},
                                  {proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, "32"},
                                  {kLevel1, "2000"},
                                  {kLevel2, "200"},
                                  {proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, "400000"},
                                  {proxima::PARAM_HC_COMMON_LEVEL_CNT, "2"},
                                  {proxima::PARAM_HC_COMMON_MAX_DOC_CNT, "100000000"},
                                  {proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, "10000000"},
                                  {proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, "2000000000"},
                                  {PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, "0.1"}};

    InnerTestInc(params);
}

void AithetaIncTest::TestGraph() {
    util::KeyValueMap params = {{DIMENSION, "72"},
                                  {INDEX_TYPE, INDEX_TYPE_GRAPH},
                                  {BUILD_METRIC_TYPE, INNER_PRODUCT},
                                  {SEARCH_METRIC_TYPE, INNER_PRODUCT},
                                  {USE_LINEAR_THRESHOLD, "1"},  // TODO(luoli.hn) 退化为LR才可以过
                                  {proxima::PARAM_GRAPH_COMMON_MAX_DOC_CNT, "100000000"},
                                  {proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, "hnsw"},
                                  {proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, "2000"},
                                  {proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT, "200"},
                                  {proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA, "2000000000"},
                                  {proxima::PARAM_GRAPH_COMMON_WITH_QUANTIZER, "false"},
                                  {proxima::PARAM_HNSW_BUILDER_MAX_LEVEL, "42"},
                                  {proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR, "30"},
                                  {proxima::PARAM_HNSW_SEARCHER_EF, "100"},
                                  {proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION, "100"},
                                  {proxima::PARAM_HNSW_BUILDER_UPPER_NEIGHBOR_CNT, "100"}};
    InnerTestInc(params);
}

void AithetaIncTest::TestBoundary() {
    util::KeyValueMap params = {{DIMENSION, "8"},          {INDEX_TYPE, INDEX_TYPE_HC},
                                  {BUILD_METRIC_TYPE, L2},   {SEARCH_METRIC_TYPE, INNER_PRODUCT},
                                  {USE_DYNAMIC_PARAMS, "1"}, {USE_LINEAR_THRESHOLD, "1"}};

    {
        const auto &partDir = GET_PARTITION_DIRECTORY();
        DirectoryPtr indexerDir = partDir->MakeDirectory("./indexer");
        IndexerPtr indexer(mFactory->createIndexer(params));
        ASSERT_TRUE(indexer->Dump(indexerDir));

        IndexReducerPtr reducer(mFactory->createIndexReducer(params));
        DirectoryPtr reducerDir = partDir->MakeDirectory("./reducer");
        SegmentMergeInfos mergeInfos({SegmentMergeInfo(0, SegmentInfo(), 0U, 0)});
        vector<DirectoryPtr> directories = {reducerDir};
        OutputSegmentMergeInfos outputSegMergeInfos;
        EXPECT_EQ(0, reducer->EstimateMemoryUse(directories, mergeInfos, outputSegMergeInfos, true));
        IndexReduceItemPtr reduceItem(reducer->CreateReduceItem());
        ASSERT_TRUE(reduceItem->LoadIndex(indexerDir));
        docid_t base = 0;
        std::vector<docid_t> newDocIds;
        DocIdMap docIdMap = CreateDocIdMap(base, newDocIds);
        ASSERT_TRUE(reduceItem->UpdateDocId(docIdMap));
        // suppose docCount is legal
        mergeInfos[0].segmentInfo.docCount = 1;
        IE_NAMESPACE(index)::ReduceResource rsc(mergeInfos, ReclaimMapPtr(new ReclaimMap), true);
        OutputSegmentMergeInfo outputSegmentMergeInfo;
        outputSegmentMergeInfo.directory = reducerDir;
        vector<IndexReduceItemPtr> reduceItems = {reduceItem};
        ASSERT_TRUE(reducer->Reduce(reduceItems, {outputSegmentMergeInfo}, false, rsc));

        IndexRetrieverPtr retriever(mFactory->createIndexRetriever(params));
        IndexSegmentRetrieverPtr segRetriever(mFactory->createIndexSegmentRetriever(params));
        EXPECT_EQ(InnerGetIndexSegmentLoad4RetrieveMemoryUse(reducerDir->GetPath()),
                  segRetriever->EstimateLoadMemoryUse(reducerDir));
        EXPECT_TRUE(segRetriever->Open(reducerDir));
        std::vector<IndexSegmentRetrieverPtr> segRetrievers = {segRetriever};
        DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
        std::vector<docid_t> baseDocIds = {0};
        EXPECT_TRUE(retriever->Init(deletionMapReader, segRetrievers, baseDocIds));

        autil::mem_pool::Pool pool;
        std::string q;
        std::string query;
        std::string topK;
        common::Term term;
        vector<SegmentMatchInfo> segMatchInfos;
        MatchInfoPtr matchInfo;

        q = "4#1.01,1.01,1.01,1.01,1.01,1.01,1.01,1.0";
        topK = "1";
        query = q + "&n=" + topK;
        term.SetWord(query);
        segMatchInfos = retriever->Search(term, &pool);
        matchInfo = segMatchInfos[0].matchInfo;
        EXPECT_EQ(0, matchInfo->matchCount);

        FileSystemWrapper::DeleteDir(indexerDir->GetPath());
        FileSystemWrapper::DeleteDir(reducerDir->GetPath());
    }
    {
        vector<vector<string>> data = {{"1", "4", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,1.0"},
                                       {"2", "2", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,2.0"},
                                       {"3", "1", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,5.0"},
                                       {"4", "3", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,6.0"},
                                       {"5", "1", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,1.0"},
                                       {"6", "2", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,2.0"},
                                       {"7", "2", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,3.0"},
                                       {"8", "1", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,4.0"},
                                       {"9", "1", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,5.0"},
                                       {"10", "3", "1.01,1.01,1.01,1.01,1.01,1.01,1.01,6.0"}};

        const auto &partDir = GET_PARTITION_DIRECTORY();
        DirectoryPtr indexerDir = partDir->MakeDirectory("./_indexer");
        IndexerPtr indexer(mFactory->createIndexer(params));
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
            ASSERT_TRUE(indexer->Build(fields, docId));
            ++docId;
        }
    }
}

DirectoryPtr AithetaIncTest::CreateReduceItem(const KeyValueMap &params, string srcs, docid_t beginId,
                                              const DocIdMap &docIdMap, IndexReduceItemPtr &reduceItem) {
    const auto &partDir = GET_PARTITION_DIRECTORY();
    DirectoryPtr indexerDir = partDir->MakeDirectory("indexer_" + srcs);

    IndexerPtr indexer(mFactory->createIndexer(params));
    if (nullptr == indexer) {
        IE_LOG(ERROR, "create indexer failed");
        return indexerDir;
    }

    const string path = "./aitheta_indexer/plugins/aitheta/test/" + srcs;
    fstream input(path);
    if (!input.is_open()) {
        IE_LOG(ERROR, "failed to open file");
        return indexerDir;
    }

    while (!input.eof()) {
        string doc;
        std::getline(input, doc);
        vector<string> strs(autil::StringUtil::split(doc, ";"));
        if (3U != strs.size()) {
            IE_LOG(ERROR, "strs.size() err");
            return indexerDir;
        }
        vector<const Field *> fields;
        vector<IndexRawField> rawFields(3);
        for (size_t i = 0; i < 3U; ++i) {
            rawFields[i].SetData(autil::ConstString(strs[i]));
            fields.push_back(&rawFields[i]);
        }
        if (!indexer->Build(fields, beginId++)) {
            IE_LOG(ERROR, "build failed");
            // return indexerDir;
        }
    }

    if (!indexer->Dump(indexerDir)) {
        IE_LOG(ERROR, "indexer dump failed");
        return indexerDir;
    }

    IndexReducerPtr reducer(mFactory->createIndexReducer(params));
    reduceItem = reducer->CreateReduceItem();
    if (!reduceItem->LoadIndex(indexerDir)) {
        IE_LOG(ERROR, "load index failed");
        return indexerDir;
    }
    if (!reduceItem->UpdateDocId(docIdMap)) {
        IE_LOG(ERROR, "update docid failed");
        return indexerDir;
    }
    return indexerDir;
}

IE_NAMESPACE_END(aitheta_plugin);
