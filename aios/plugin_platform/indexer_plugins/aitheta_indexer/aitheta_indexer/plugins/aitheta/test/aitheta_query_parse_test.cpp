#include "aitheta_indexer/plugins/aitheta/test/aitheta_query_parse_test.h"

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
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(aitheta_plugin);

void AithetaQueryParseTest::TestPaseQueryWithScoreFilter() {
    {
        util::KeyValueMap params = {{DIMENSION, "72"}};
        AithetaIndexRetriever aithetaIndexRetriever(params);
        {
            std::string query =
                "50012772#0,0,0,0,0.291818,0.049089,0.576867, "
                "0.310323,0.00979928,0,0.398244,0,0.128537,0,0.0356869,0,0,0,0."
                "0495991,0,0.078342,0,0,0.186807,0,0.145816,0.105553,0,0,0."
                "00566058,0,0.478819,0,0,0,0.0128467,0,0,0,0,0.034456,0,0,0,0."
                "535329,0.0230616,0,0,0,0.030542,0,0,0,0.0790533,0.153851,0."
                "00635181,0,0,0,0.492543,0,0,0,0,0,0.21522,0,0,0,0,0,0.11&n=10";

            int topK = 0;
            bool needScoreFilter = false;
            float score = 0.0;
            vector<QueryInfo> queryInfos;
            aithetaIndexRetriever.ParseQuery(query, queryInfos, topK, needScoreFilter, score);

            EXPECT_EQ(10, topK);
            EXPECT_EQ(false, needScoreFilter);
            EXPECT_FLOAT_EQ(0.0, score);
            EXPECT_EQ(1, queryInfos.size());

            EXPECT_EQ(50012772, queryInfos[0].mCatId);
            EXPECT_FLOAT_EQ(0.0, queryInfos[0].mEmbedding.get()[0]);
            EXPECT_FLOAT_EQ(0.11, queryInfos[0].mEmbedding.get()[71]);
            EXPECT_FLOAT_EQ(0.128537, queryInfos[0].mEmbedding.get()[12]);
        }
        {
            std::string query =
                "50012772,50012773#0,0,0,0,0.291818,0.049089,0.576867, "
                "0.310323,0.00979928,0,0.398244,0,0.128537,0,0.0356869,0,0,0,0."
                "0495991,0,0.078342,0,0,0.186807,0,0.145816,0.105553,0,0,0."
                "00566058,0,0.478819,0,0,0,0.0128467,0,0,0,0,0.034456,0,0,0,0."
                "535329,0.0230616,0,0,0,0.030542,0,0,0,0.0790533,0.153851,0."
                "00635181,0,0,0,0.492543,0,0,0,0,0,0.21522,0,0,0,0,0,0.11&n=10&"
                "sf=0.534";

            int topK = 0;
            bool needScoreFilter = false;
            float score = 0.0;
            vector<QueryInfo> queryInfos;
            aithetaIndexRetriever.ParseQuery(query, queryInfos, topK, needScoreFilter, score);

            EXPECT_EQ(10, topK);
            EXPECT_EQ(true, needScoreFilter);
            EXPECT_FLOAT_EQ(0.534, score);
            EXPECT_EQ(2, queryInfos.size());

            EXPECT_EQ(50012772, queryInfos[1].mCatId);
            EXPECT_EQ(50012773, queryInfos[0].mCatId);
            EXPECT_FLOAT_EQ(0.0, queryInfos[0].mEmbedding.get()[0]);
            EXPECT_FLOAT_EQ(0.11, queryInfos[0].mEmbedding.get()[71]);
            EXPECT_FLOAT_EQ(0.128537, queryInfos[0].mEmbedding.get()[12]);
        }
    }
    {
        util::KeyValueMap params = {{DIMENSION, "72"}, {TOPK_FLAG, "&t="}, {SCORE_FILTER_FLAG, "&score="}};
        AithetaIndexRetriever aithetaIndexRetriever(params);

        {
            // test multi category(with blank space)
            std::string query =
                "50012772, 50012773# 0,0,0,0, 0.291818,0.049089, 0.576867, "
                "0.310323,0.00979928,0,0.398244,0,0.128537,0,0.0356869,0,0,0,0."
                "0495991,0,0.078342,0,0,0.186807,0,0.145816,0.105553,0,0,0."
                "00566058,0,0.478819,0,0,0,0.0128467,0,0,0,0,0.034456,0,0,0,0."
                "535329,0.0230616,0,0,0,0.030542,0,0,0,0.0790533,0.153851,0."
                "00635181,0,0,0,0.492543,0,0,0,0,0,0.21522,0,0,0,0,0,0.11&t=10&"
                "score=-1.57";

            int topK = 0;
            bool needScoreFilter = false;
            float score = 0.0;
            vector<QueryInfo> queryInfos;
            aithetaIndexRetriever.ParseQuery(query, queryInfos, topK, needScoreFilter, score);

            EXPECT_EQ(10, topK);
            EXPECT_EQ(true, needScoreFilter);
            EXPECT_FLOAT_EQ(-1.57, score);
            EXPECT_EQ(2, queryInfos.size());

            EXPECT_EQ(50012772, queryInfos[1].mCatId);
            EXPECT_EQ(50012773, queryInfos[0].mCatId);
            EXPECT_FLOAT_EQ(0.0, queryInfos[0].mEmbedding.get()[0]);
            EXPECT_FLOAT_EQ(0.11, queryInfos[0].mEmbedding.get()[71]);
            EXPECT_FLOAT_EQ(0.128537, queryInfos[0].mEmbedding.get()[12]);
        }
    }
    {
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

        fstream input(relativeInputPath);
        ASSERT_TRUE(input.is_open());
        autil::mem_pool::Pool pool;
        docid_t docId = 0;
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

        std::vector<IndexSegmentRetrieverPtr> retrievers;
        retrievers.push_back(indexSegRetriever);
        DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
        std::vector<docid_t> baseDocIds;
        baseDocIds.push_back(0);
        indexRetriever->Init(deletionMapReader, retrievers, baseDocIds);

        std::string q;
        std::string query;
        std::string topK;
        std::string score;
        common::Term term;
        vector<SegmentMatchInfo> segMatchInfos;
        MatchInfoPtr matchInfo;

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

        topK = "5";
        score = "30";
        query = q + "&n=" + topK + "&sf=" + score;
        term.SetWord(query);
        segMatchInfos = indexRetriever->Search(term, &pool);
        matchInfo = segMatchInfos[0].matchInfo;
        EXPECT_EQ(2, matchInfo->matchCount);
        EXPECT_EQ(0, matchInfo->docIds[0]);
        EXPECT_EQ(1, matchInfo->docIds[1]);
        float fscore = 0.0;
        memcpy(&fscore, matchInfo->matchValues + 0, sizeof(fscore));
        EXPECT_FLOAT_EQ(0.0, fscore);
        memcpy(&fscore, matchInfo->matchValues + 1, sizeof(fscore));
        EXPECT_FLOAT_EQ(8.6832256, fscore);

        topK = "1";
        score = "0.0";
        query = q + "&n=" + topK + "&sf=" + score;
        term.SetWord(query);
        segMatchInfos = indexRetriever->Search(term, &pool);
        matchInfo = segMatchInfos[0].matchInfo;
        EXPECT_EQ(1, matchInfo->matchCount);

        FileSystemWrapper::DeleteDir(indexerDir->GetPath());
        FileSystemWrapper::DeleteDir(reducerDir->GetPath());
    }
    {
        util::KeyValueMap params = {{DIMENSION, "72"},
                                      {INDEX_TYPE, INDEX_TYPE_HC},
                                      {BUILD_METRIC_TYPE, L2},
                                      {SEARCH_METRIC_TYPE, INNER_PRODUCT},
                                      {proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, "32"},
                                      {proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1", "2000"},
                                      {proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2", "200"},
                                      {proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, "400000"},
                                      {proxima::PARAM_HC_COMMON_LEVEL_CNT, "2"},
                                      {proxima::PARAM_HC_COMMON_MAX_DOC_CNT, "100000000"},
                                      {proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, "10000000"},
                                      {proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, "2000000000"},
                                      {PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, "0.1"}};
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

        fstream input(relativeInputPath);
        ASSERT_TRUE(input.is_open());
        autil::mem_pool::Pool pool;
        docid_t docId = 0;
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
        DirectoryPtr indexerDir = partDir->MakeDirectory(inputName + "_indexer1");
        ASSERT_TRUE(indexer->Dump(indexerDir));

        // test reduceItem loadIndex
        IndexReduceItemPtr reduceItem(reducer->CreateReduceItem());
        ASSERT_TRUE(reduceItem->LoadIndex(indexerDir));

        // test reducer reduce
        std::vector<IndexReduceItemPtr> reduceItems = {reduceItem};
        DirectoryPtr reducerDir = partDir->MakeDirectory(inputName + "_reducer1");
        docid_t base = 0;
        SegmentMergeInfos mergeInfos({SegmentMergeInfo(0, SegmentInfo(), 0U, base)});
        mergeInfos[0].segmentInfo.docCount = 7;
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
        std::string score;
        common::Term term;
        vector<SegmentMatchInfo> segMatchInfos;
        MatchInfoPtr matchInfo;

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

        topK = "5";
        score = "30.0";
        query = q + "&n=" + topK + "&sf=" + score;
        term.SetWord(query);
        segMatchInfos = indexRetriever->Search(term, &pool);
        matchInfo = segMatchInfos[0].matchInfo;
        EXPECT_EQ(3, matchInfo->matchCount);
        EXPECT_EQ(0, matchInfo->docIds[0]);
        EXPECT_EQ(1, matchInfo->docIds[1]);
        EXPECT_EQ(2, matchInfo->docIds[2]);
        float fscore = 0.0;
        memcpy(&fscore, matchInfo->matchValues + 0, sizeof(fscore));
        EXPECT_FLOAT_EQ(63.752316, fscore);
        memcpy(&fscore, matchInfo->matchValues + 1, sizeof(fscore));
        EXPECT_FLOAT_EQ(59.37606, fscore);
        memcpy(&fscore, matchInfo->matchValues + 2, sizeof(fscore));
        EXPECT_FLOAT_EQ(43.367054, fscore);

        topK = "1";
        score = "1000.0";
        query = q + "&n=" + topK + "&sf=" + score;
        term.SetWord(query);
        segMatchInfos = indexRetriever->Search(term, &pool);
        matchInfo = segMatchInfos[0].matchInfo;
        EXPECT_EQ(0, matchInfo->matchCount);
    }
}

void AithetaQueryParseTest::TestParseQuery() {
    {
        util::KeyValueMap params = {
            {DIMENSION, "72"},         {QUERY_SEPARATOR, ";"},  {CATEGORY_EMBEDDING_SEPARATOR, "#"},
            {CATEGORY_SEPARATOR, ","}, {EXTEND_SEPARATOR, ":"}, {EMBEDDING_SEPARATOR, ","},
            {TOPK_FLAG, "&t="}};
        AithetaIndexRetriever aithetaIndexRetriever(params);

        {
            // test multi category(with blank space)
            std::string query =
                "50012772, 50012773# 0,0,0,0, 0.291818,0.049089, 0.576867, "
                "0.310323,0.00979928,0,0.398244,0,0.128537,0,0.0356869,0,0,0,0."
                "0495991,0,0.078342,0,0,0.186807,0,0.145816,0.105553,0,0,0."
                "00566058,0,0.478819,0,0,0,0.0128467,0,0,0,0,0.034456,0,0,0,0."
                "535329,0.0230616,0,0,0,0.030542,0,0,0,0.0790533,0.153851,0."
                "00635181,0,0,0,0.492543,0,0,0,0,0,0.21522,0,0,0,0,0,0.11&t=10";

            int topK = 0;
            bool needScoreFilter = false;
            float score = 0.0;
            vector<QueryInfo> queryInfos;
            aithetaIndexRetriever.ParseQuery(query, queryInfos, topK, needScoreFilter, score);

            EXPECT_EQ(10, topK);
            EXPECT_EQ(2, queryInfos.size());

            EXPECT_EQ(50012773, queryInfos[0].mCatId);
            EXPECT_EQ(50012772, queryInfos[1].mCatId);
            EXPECT_FLOAT_EQ(0.0, queryInfos[0].mEmbedding.get()[0]);
            EXPECT_FLOAT_EQ(0.11, queryInfos[0].mEmbedding.get()[71]);
            EXPECT_FLOAT_EQ(0.128537, queryInfos[0].mEmbedding.get()[12]);
        }

        {
            //  test non category
            std::string query =
                "0,0,0,0,0.2918,0.049089,0.576867,0.310323,0.00979928,0,0."
                "398244,0,0.128537,0,0.0356869,0,0,0,0.0495991,0,0.078342,0,0,"
                "0.186807,0,0.145816,0.105553,0,0,0.00566058,0,0.478819,0,0,"
                "0,0.0128467,0,0,0,0,0.034456,0,0,0,0.535329,0.0230616,0,0,0,0."
                "030542,0,0,0,0.0790533,0.153851,0.00635181,0,0,0,0.492543,0,0,"
                "0,0,0,0.21522,0,0,0,0,0,0;50012772#0,0,0,0,0.291818,0.049089,"
                "0.576867,0.310323,0.00979928,0,0.398244,0,0.128537,0,0."
                "0356869,0,0,0,0.0495991,0,0.078342,0,0,0.186807,0,0.145816,0."
                "105553,0,0,0.00566058,0,0.478819,0,0,0,0.0128467,0,0,0,0,0."
                "034456,0,0,0,0.535329,0.0230616,0,0,0,0.030542,0,0,0,0."
                "0790533,0.153851,0.00635181,0,0,0,0.492543,0,0,0,0,0,0.21522,"
                "0,0,0,0,0,0&t=1";

            int topK = 0;
            bool needScoreFilter = false;
            float score = 0.0;
            vector<QueryInfo> queryInfos;
            aithetaIndexRetriever.ParseQuery(query, queryInfos, topK, needScoreFilter, score);

            EXPECT_EQ(1, topK);
            ASSERT_EQ(2, queryInfos.size());

            EXPECT_EQ(50012772, queryInfos[0].mCatId);
            EXPECT_FLOAT_EQ(0.0, queryInfos[0].mEmbedding.get()[0]);
            EXPECT_FLOAT_EQ(0.0, queryInfos[0].mEmbedding.get()[71]);
            EXPECT_FLOAT_EQ(0.291818, queryInfos[0].mEmbedding.get()[4]);

            EXPECT_EQ(DEFAULT_CATEGORY_ID, queryInfos[1].mCatId);
            EXPECT_FLOAT_EQ(0.0, queryInfos[1].mEmbedding.get()[0]);
            EXPECT_FLOAT_EQ(0.0, queryInfos[1].mEmbedding.get()[71]);
            EXPECT_FLOAT_EQ(0.049089, queryInfos[1].mEmbedding.get()[5]);
        }
        // test without topk
        {
            std::string query =
                "0,0,0,0,0.291818,0.049089,0.576867,0.310323,0.00979928,0,0."
                "398244,0,0.128537,0,0.0356869,0,0,0,0.0495991,0,0.078342,0,0,"
                "0.186807,0,0.145816,0.105553,0,0,0.00566058,0,0.478819,0,0,"
                "0,0.0128467,0,0,0,0,0.034456,0,0,0,0.535329,0.0230616,0,0,0,0."
                "030542,0,0,0,0.0790533,0.153851,0.00635181,0,0,0,0.492543,0,0,"
                "0,0,0,0.21522,0,0,0,0,0,0;0,0,0,0,0.291818,0.049089,0.576867,"
                "0.310323,0.00979928,0,0.398244,0,0.128537,0,0.0356869,0,0,0,0."
                "0495991,0,0.078342,0,0,0.186807,0,0.145816,0.105553,0,0,0."
                "00566058,0,0.478819,0,0,0,0.0128467,0,0,0,0,0.034456,0,0,0,0."
                "535329,0.0230616,0,0,0,0.030542,0,0,0,0.0790533,0.153851,0."
                "00635181,0,0,0,0.492543,0,0,0,0,0,0.21522,0,0,0,0,0,0";

            int topK = 0;
            bool needScoreFilter = false;
            float score = 0.0;
            vector<QueryInfo> queryInfos;
            aithetaIndexRetriever.ParseQuery(query, queryInfos, topK, needScoreFilter, score);

            EXPECT_EQ(0, topK);
            ASSERT_EQ(0, queryInfos.size());
        }
        // test with wrong topk string
        {
            std::string query =
                "0,0,0,0,0.049089,0.576867,0.310323,0.00979928,0,0.398244,0,0."
                "128537,0,0.0356869,0,0,0,0.0495991,0,0.078342,0,0,0.186807,0,"
                "0.145816,0.105553,0,0,0.00566058,0,0.478819,0,0,0,0.0128467,0,"
                "0,0,0,0.034456,0,0,0,0.535329,0.0230616,0,0,0,0.030542,0,0,0,"
                "0.0790533,0.153851,0.00635181,0,0,0,0.492543,0,0,0,0,0,0."
                "21522,0,0,0,0,0,0;0,0,0,0,0.291818,0.049089,0.576867,0.310323,"
                "0.00979928,0,0.398244,0,0.128537,0,0.0356869,0,0,0,0.0495991,"
                "0,0.078342,0,0,0.186807,0,0.145816,0.105553,0,0,0.00566058,0,"
                "0.478819,0,0,0,0.0128467,0,0,0,0,0.034456,0,0,0,0.535329,0."
                "0230616,0,0,0,0.030542,0,0,0,0.0790533,0.153851,0.00635181,0,"
                "0,0,0.492543,0,0,0,0,0,0.21522,0,0,0,0,0,0&n=2";

            int topK = 0;
            bool needScoreFilter = false;
            float score = 0.0;
            vector<QueryInfo> queryInfos;
            aithetaIndexRetriever.ParseQuery(query, queryInfos, topK, needScoreFilter, score);

            EXPECT_EQ(0, topK);
            ASSERT_EQ(0, queryInfos.size());
        }
    }
    // test different seperator
    {
        KeyValueMap params = {{DIMENSION, "72"},         {QUERY_SEPARATOR, ";"},  {CATEGORY_EMBEDDING_SEPARATOR, "?"},
                              {CATEGORY_SEPARATOR, "#"}, {EXTEND_SEPARATOR, ":"}, {EMBEDDING_SEPARATOR, "_"},
                              {TOPK_FLAG, "&t="}};
        AithetaIndexRetriever aithetaIndexRetriever(params);
        {
            string query =
                "50012772#50012773?0_0_0_0_0.291818_0.049089_0."
                "576867_0.310323_0.00979928_0_0.398244_0_0.128537_0_0.0356869_"
                "0_0_0_0.0495991_0_0.078342_0_0_0.186807_0_0.145816_0.105553_0_"
                "0_0.00566058_0_0.478819_0_0_0_0.0128467_0_0_0_0_0.034456_0_0_"
                "0_0.535329_0.0230616_0_0_0_0.030542_0_0_0_0.0790533_0.153851_"
                "0.00635181_0_0_0_0.492543_0_0_0_0_0_0.21522_0_0_0_0_0_0&t="
                "1001212";

            int topK = 0;
            bool needScoreFilter = false;
            float score = 0.0;
            vector<QueryInfo> queryInfos;
            aithetaIndexRetriever.ParseQuery(query, queryInfos, topK, needScoreFilter, score);

            EXPECT_EQ(1001212, topK);
            ASSERT_EQ(2, queryInfos.size());
            EXPECT_EQ(50012773, queryInfos[0].mCatId);
            EXPECT_FLOAT_EQ(0.0, queryInfos[0].mEmbedding.get()[0]);
            EXPECT_FLOAT_EQ(0.049089, queryInfos[0].mEmbedding.get()[5]);
            EXPECT_FLOAT_EQ(0.21522, queryInfos[0].mEmbedding.get()[65]);
            EXPECT_FLOAT_EQ(0.0, queryInfos[0].mEmbedding.get()[71]);
        }
    }

    {
        util::KeyValueMap params = {{DIMENSION, "72"}};
        AithetaIndexRetriever aithetaIndexRetriever(params);
        {
            std::string query =
                "50012772,50012773#0,0,0,0,0.291818,0.049089,0.576867,0.310323,"
                "0.00979928,0,0.398244,0,0.128537,0,0.0356869,0,0,0,0.0495991,"
                "0,0.078342,0,0,0.186807,0,0.145816,0.105553,0,0,0.00566058,0,"
                "0.478819,0,0,0,0.0128467,0,0,0,0,0.034456,0,0,0,0.535329,0."
                "0230616,0,0,0,0.030542,0,0,0,0.0790533,0.153851,0.00635181,0,"
                "0,0,0.492543,0,0,0,0,0,0.21522,0,0,0,0,0,0.11&n=10";

            int topK = 0;
            bool needScoreFilter = false;
            float score = 0.0;
            vector<QueryInfo> queryInfos;
            aithetaIndexRetriever.ParseQuery(query, queryInfos, topK, needScoreFilter, score);

            EXPECT_EQ(10, topK);
            EXPECT_EQ(2, queryInfos.size());

            EXPECT_EQ(50012773, queryInfos[0].mCatId);
            EXPECT_EQ(50012772, queryInfos[1].mCatId);
            EXPECT_FLOAT_EQ(0.0, queryInfos[0].mEmbedding.get()[0]);
            EXPECT_FLOAT_EQ(0.11, queryInfos[0].mEmbedding.get()[71]);
            EXPECT_FLOAT_EQ(0.128537, queryInfos[0].mEmbedding.get()[12]);
        }
    }
}

IE_NAMESPACE_END(aitheta_plugin);
