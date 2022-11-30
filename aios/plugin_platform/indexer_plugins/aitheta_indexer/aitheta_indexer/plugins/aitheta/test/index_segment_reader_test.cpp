#include "aitheta_indexer/plugins/aitheta/test/index_segment_reader_test.h"
#include <proxima/common/params_define.h>
#include "aitheta_indexer/plugins/aitheta/index_segment_reader.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_NAMESPACE_USE(index);

void IndexSegmentReaderTest::TestSequentialSearch() {
}

void IndexSegmentReaderTest::TestParallelSearch() {
    util::KeyValueMap params = {{DIMENSION, "72"},
                                {INDEX_TYPE, INDEX_TYPE_HC},
                                {BUILD_METRIC_TYPE, L2},
                                {SEARCH_METRIC_TYPE, INNER_PRODUCT},
                                {ENABLE_PARALLEL_SEARCH, "true"},
                                {PARALLEL_SEARCH_QUEUE_SIZE, "256"},
                                {PARALLEL_SEARCH_THREAD_NUM, "32"}};

    const string testDataPath = "testdata/index_segment_reader_test/8docs_72dim_multi_catid.txt";
    IndexRetrieverPtr searcher;
    string indexPath;
    ASSERT_TRUE(CreateSearcher(params, {testDataPath}, indexPath, searcher));

    vector<std::pair<docid_t, float>> results;

    const string kEmbedding =
        "1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,"
        "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0";
    {
        string query = "50012772#" + kEmbedding + "&n=3";
        ASSERT_TRUE(Search(searcher, query, results));
        ASSERT_EQ(3, results.size());
        ASSERT_EQ(0, results[0].first);
        ASSERT_EQ(1, results[1].first);
        ASSERT_EQ(5, results[2].first);
        EXPECT_FLOAT_EQ(7, results[0].second);
        EXPECT_FLOAT_EQ(6, results[1].second);
        EXPECT_FLOAT_EQ(2, results[2].second);

        IE_LOG(INFO, "sucecess in with [%s]", query.c_str());
    }
    {
        string query = "50012772#" + kEmbedding + "&n=3&sf=6";
        ASSERT_TRUE(Search(searcher, query, results));
        ASSERT_EQ(2, results.size());
        ASSERT_EQ(0, results[0].first);
        ASSERT_EQ(1, results[1].first);
        EXPECT_FLOAT_EQ(7, results[0].second);
        EXPECT_FLOAT_EQ(6, results[1].second);

        IE_LOG(INFO, "sucecess in with [%s]", query.c_str());
    }
    {
        string query = "50012772,50012773,50012774#" + kEmbedding + "&n=2";
        ASSERT_TRUE(Search(searcher, query, results));
        ASSERT_EQ(4, results.size());
        ASSERT_EQ(0, results[0].first);
        ASSERT_EQ(1, results[1].first);
        ASSERT_EQ(2, results[2].first);
        ASSERT_EQ(3, results[3].first);
        EXPECT_FLOAT_EQ(7, results[0].second);
        EXPECT_FLOAT_EQ(6, results[1].second);
        EXPECT_FLOAT_EQ(5, results[2].second);
        EXPECT_FLOAT_EQ(4, results[3].second);
        IE_LOG(INFO, "sucecess in with [%s]", query.c_str());
    }
}
IE_NAMESPACE_END(aitheta_plugin);
