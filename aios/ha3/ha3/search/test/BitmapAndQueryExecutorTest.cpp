#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/BitmapAndQueryExecutor.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/rank/MatchData.h>
#include <ha3/search/test/QueryExecutorTestHelper.h>

using namespace std;
USE_HA3_NAMESPACE(common);
IE_NAMESPACE_USE(index);
BEGIN_HA3_NAMESPACE(search);

class BitmapAndQueryExecutorTest : public TESTBASE {
public:
    BitmapAndQueryExecutorTest();
    ~BitmapAndQueryExecutorTest();
public:
    void setUp();
    void tearDown();
protected:
    void internalTest(index::FakeIndex &fakeIndex, 
                      const std::string &query, 
                      const std::string &expectedIdsStr,
                      bool hasSubExecutor = false);
    QueryExecutor* createExecutor(
            IndexPartitionReaderWrapper* wrapper, const std::string &queryStr);
protected:
    IndexPartitionReaderWrapperPtr _indexPartReader;
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, BitmapAndQueryExecutorTest);


BitmapAndQueryExecutorTest::BitmapAndQueryExecutorTest() {
    _pool = new autil::mem_pool::Pool(1024);
}

BitmapAndQueryExecutorTest::~BitmapAndQueryExecutorTest() {
    delete _pool;
}

void BitmapAndQueryExecutorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void BitmapAndQueryExecutorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(BitmapAndQueryExecutorTest, testTwoBitmapPostings) { 
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["bitmap_index1"] = "mp3:1;3;5;6;10\n";
    fakeIndex.indexes["bitmap_index2"] = "mp4:1;2;5;7;10\n";
    string query = "bitmap_index1:mp3 AND bitmap_index2:mp4";
    internalTest(fakeIndex, query, "1,5,10");
}

TEST_F(BitmapAndQueryExecutorTest, testAndnotTermInQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["bitmap_index1"] = "mp3:1;3;5;6;10\n";
    fakeIndex.indexes["bitmap_index2"] = "mp4:1;2;5;7;10\n";
    fakeIndex.indexes["p4delta_index3"] = "mp5:1;4;5;8;10\n";
    string query = "bitmap_index1:mp3 AND (bitmap_index2:mp4 ANDNOT p4delta_index3:mp5)";
    internalTest(fakeIndex, query, "");
}

TEST_F(BitmapAndQueryExecutorTest, testRankTermInQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["bitmap_index1"] = "mp3:1;3;5;6;10\n";
    fakeIndex.indexes["bitmap_index2"] = "mp4:1;2;5;7;10\n";
    fakeIndex.indexes["p4delta_index3"] = "mp5:1;4;5;8;10\n";
    string query = "bitmap_index1:mp3 AND (bitmap_index2:mp4 RANK p4delta_index3:mp5)";
    internalTest(fakeIndex, query, "1,5,10");
}

TEST_F(BitmapAndQueryExecutorTest, testNoResult) { 
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["bitmap_index1"] = "mp3:1;3;5;6;10\n";
    fakeIndex.indexes["bitmap_index2"] = "mp4:1;2;5;7;10\n";
    fakeIndex.indexes["p4delta_index3"] = "mp5:4;8;9;12\n";
    string query = "bitmap_index1:mp3 AND (bitmap_index2:mp4 AND p4delta_index3:mp5)";
    internalTest(fakeIndex, query, "");
}

TEST_F(BitmapAndQueryExecutorTest, testOrTermInQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["bitmap_index1"] = "mp3:1;3;5;6;10\n";
    fakeIndex.indexes["bitmap_index2"] = "mp4:1;2;5;7;10\n";
    fakeIndex.indexes["p4delta_index3"] = "mp5:1;4;5;8;10\n";
    string query = "bitmap_index1:mp3 AND (bitmap_index2:mp4 OR p4delta_index3:mp5)";
    internalTest(fakeIndex, query, "1,5,10");
}

TEST_F(BitmapAndQueryExecutorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["bitmap_index1"] = "mp3:1;3;5;6;10\n";
    fakeIndex.indexes["p4delta_index2"] = "mp4:1;2;5;7;10\n";
    string query = "bitmap_index1:mp3 AND p4delta_index2:mp4";

    internalTest(fakeIndex, query, "1,5,10");
}

TEST_F(BitmapAndQueryExecutorTest, testReset) {
    FakeIndex fakeIndex;
    fakeIndex.indexes["bitmap_index1"] = "bitmap_term:1;2;3\n";
    fakeIndex.indexes["p4delta_index2"] = "term1:1;2;3\n"
                                          "term2:1;2;3\n";
    string query = "bitmap_index1:bitmap_term AND p4delta_index2:term1 AND p4delta_index2:term2";
    _indexPartReader = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);

    QueryExecutor* executor = createExecutor(_indexPartReader.get(), query);
    ASSERT_EQ(docid_t(1), executor->legacySeek(0));
    ASSERT_EQ(docid_t(2), executor->legacySeek(2));
    executor->reset();
    ASSERT_EQ(docid_t(1), executor->legacySeek(0));
    ASSERT_EQ(docid_t(2), executor->legacySeek(2));
    POOL_DELETE_CLASS(executor);
}

TEST_F(BitmapAndQueryExecutorTest, testSubDocSeek) { 
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["bitmap_index1"] = "mp3:0,1,2,3\n";
    fakeIndex.indexes["index2"] = "mp4:0,1\n";
    fakeIndex.indexes["sub_index3"] = "mp4:1\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,10,15");

    string query = "bitmap_index1:mp3 AND index2:mp4 AND sub_index3:mp4";

    _indexPartReader = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexPartReader->setTopK(1000);   
    QueryExecutor* executor = createExecutor(_indexPartReader.get(), query);
    ASSERT_TRUE(executor->hasSubDocExecutor());
    ASSERT_EQ(docid_t(0), executor->legacySeek(0));
    ASSERT_TRUE(!executor->isMainDocHit(0));

    ASSERT_EQ(docid_t(1), executor->legacySeekSub(0, 0, 3));
    ASSERT_EQ(docid_t(END_DOCID), executor->legacySeekSub(0, 2, 3));

    ASSERT_EQ(END_DOCID, executor->legacySeek(1));
    POOL_DELETE_CLASS(executor);
}


QueryExecutor* BitmapAndQueryExecutorTest::createExecutor(
        IndexPartitionReaderWrapper* wrapper, const string &queryStr) {
   
    Query* query = SearcherTestHelper::createQuery(queryStr);
    unique_ptr<Query> auto_query(query);
    MatchDataManager manager;
    QueryExecutorCreator qeCreator(&manager, wrapper, _pool);
    query->accept(&qeCreator);
    return qeCreator.stealQuery();
}

void BitmapAndQueryExecutorTest::internalTest(
        FakeIndex &fakeIndex, const string &queryStr, const string &expectedIdsStr, bool hasSubExecutor) {
    _indexPartReader = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);

    _indexPartReader->setTopK(1000);   
    std::vector<docid_t> expectedIds = 
        SearcherTestHelper::covertToResultDocIds(expectedIdsStr);
    QueryExecutor* executor = createExecutor(_indexPartReader.get(), queryStr);
    ASSERT_EQ(hasSubExecutor, executor->hasSubDocExecutor());
    docid_t docid = 0;
    for (size_t i = 0; i < expectedIds.size(); ++i) {
        docid = executor->legacySeek(docid);
        ASSERT_EQ(expectedIds[i], docid);
        ++docid;
    }
    docid = executor->legacySeek(docid);
    ASSERT_EQ(END_DOCID, docid);
    POOL_DELETE_CLASS(executor);
}

END_HA3_NAMESPACE(search);

