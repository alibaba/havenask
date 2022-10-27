#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SimpleMatchDataFetcher.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/MatchDataManager.h>

using namespace std;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);

class SimpleMatchDataFetcherTest : public TESTBASE {
public:
    SimpleMatchDataFetcherTest();
    ~SimpleMatchDataFetcherTest();
public:
    void setUp();
    void tearDown();
protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, SimpleMatchDataFetcherTest);

SimpleMatchDataFetcherTest::SimpleMatchDataFetcherTest() {
}

SimpleMatchDataFetcherTest::~SimpleMatchDataFetcherTest() {
}

void SimpleMatchDataFetcherTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void SimpleMatchDataFetcherTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SimpleMatchDataFetcherTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    uint32_t termCount = 100;
    common::Ha3MatchDocAllocator allocator(&_pool);
    SimpleMatchDataFetcher fetcher;
    ASSERT_TRUE(fetcher.require(&allocator, SIMPLE_MATCH_DATA_REF, termCount) != NULL);
    matchdoc::MatchDoc matchDoc = allocator.allocate(0);
    auto fieldGroupPair = allocator.findGroup(common::HA3_MATCHDATA_GROUP);
    ASSERT_TRUE(fieldGroupPair.first != NULL);
    ASSERT_TRUE(fieldGroupPair.second);
    auto matchDataRef = fieldGroupPair.first->findReferenceWithoutType(
            SIMPLE_MATCH_DATA_REF);
    ASSERT_TRUE(matchDataRef != NULL);

    set<docid_t> matchDocs;
    matchDocs.insert(0);
    matchDocs.insert(27);
    matchDocs.insert(50);
    matchDocs.insert(57);
    matchDocs.insert(99);

    vector<QueryExecutor*> termQueryExecutors;
    for (uint32_t i = 0; i < termCount; ++i) {
        termQueryExecutors.push_back(new TermQueryExecutor(NULL, Term()));
        docid_t docId = matchDocs.find(i) != matchDocs.end() ? 0 : 1;
        termQueryExecutors[i]->setDocId(docId);
    }

    SimpleMatchData &data = fetcher._ref->getReference(matchDoc);
    // set some uninit data, since we don't re-init.
    data.setMatch(0, false);
    data.setMatch(1, true);
    data.setMatch(2, true);
    data.setMatch(90, false);
    data.setMatch(91, true);
    data.setMatch(92, true);

    fetcher.fillMatchData(termQueryExecutors, matchDoc, matchdoc::MatchDoc());

    for (uint32_t i = 0; i < termCount; ++i) {
        ASSERT_TRUE((matchDocs.find(i) != matchDocs.end()) == data.isMatch(i));
    }

    allocator.deallocate(matchDoc);
    for (uint32_t i = 0; i < termCount; ++i) {
        delete termQueryExecutors[i];
    }
}

TEST_F(SimpleMatchDataFetcherTest, testMultiQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    // query count 3 layer 4
    // 3, 2, 1
    uint32_t termCount = 6;
    uint32_t layerCount = 4;
    uint32_t queryCount = 3;
    common::Ha3MatchDocAllocator allocator(&_pool);
    SimpleMatchDataFetcher fetcher;
    ASSERT_TRUE(fetcher.require(&allocator, SIMPLE_MATCH_DATA_REF, termCount) != NULL);
    MatchDataManager manager;
    manager.setQueryCount(queryCount);
    manager._fetcher = &fetcher;
    vector<matchdoc::MatchDoc> matchDocs;
    for (uint32_t i = 0; i < layerCount; ++i) {
        matchdoc::MatchDoc matchDoc = allocator.allocate(i);
        matchDocs.push_back(matchDoc);
    }

    vector<SingleLayerSeekExecutors> &termQueryExecutors = manager._queryExecutors;
    termQueryExecutors.resize(layerCount);
    uint32_t termCounts[] = {3, 2, 1, 3};
    for (uint32_t i = 0; i < layerCount; ++i) {
        SingleLayerSeekExecutors &tmp = termQueryExecutors[i];
        for (uint32_t j = 0; j < termCounts[i]; j++) {
            tmp.push_back(new TermQueryExecutor(NULL, Term()));
        }
    }

    termQueryExecutors[0][0]->setDocId(0); // match 0
    termQueryExecutors[0][1]->setDocId(100);
    termQueryExecutors[0][2]->setDocId(0); // match 0

    termQueryExecutors[1][0]->setDocId(100);
    termQueryExecutors[1][1]->setDocId(1); // match 1

    termQueryExecutors[2][0]->setDocId(2); // match 2

    termQueryExecutors[3][0]->setDocId(100);
    termQueryExecutors[3][1]->setDocId(3); // match 3
    termQueryExecutors[3][2]->setDocId(100);

    SimpleMatchData &data = fetcher._ref->getReference(matchDocs[0]);
    SimpleMatchData &data1 = fetcher._ref->getReference(matchDocs[1]);
    SimpleMatchData &data2 = fetcher._ref->getReference(matchDocs[2]);
    SimpleMatchData &data3 = fetcher._ref->getReference(matchDocs[3]);

    fetcher.fillMatchData(termQueryExecutors[0], matchDocs[0], matchdoc::MatchDoc());
    manager.moveToLayer(1);
    fetcher.fillMatchData(termQueryExecutors[1], matchDocs[1], matchdoc::MatchDoc());
    manager.moveToLayer(2);
    fetcher.fillMatchData(termQueryExecutors[2], matchDocs[2], matchdoc::MatchDoc());
    manager.moveToLayer(3);
    fetcher.fillMatchData(termQueryExecutors[3], matchDocs[3], matchdoc::MatchDoc());

    set<uint32_t> excepts;
    excepts.insert(0);
    excepts.insert(2);
    for (uint32_t i = 0; i < termCount; ++i) {
        ASSERT_TRUE((excepts.find(i) != excepts.end()) == data.isMatch(i));
    }

    set<uint32_t> excepts1;
    excepts1.insert(4);
    for (uint32_t i = 0; i < termCount; ++i) {
        ASSERT_TRUE((excepts1.find(i) != excepts1.end()) == data1.isMatch(i));
    }

    set<uint32_t> excepts2;
    excepts2.insert(5);
    for (uint32_t i = 0; i < termCount; ++i) {
        ASSERT_TRUE((excepts2.find(i) != excepts2.end()) == data2.isMatch(i));
    }

    set<uint32_t> excepts3;
    excepts3.insert(1);
    for (uint32_t i = 0; i < termCount; ++i) {
        ASSERT_TRUE((excepts3.find(i) != excepts3.end()) == data3.isMatch(i));
    }

    for (uint32_t i = 0; i < layerCount; i++) {
        allocator.deallocate(matchDocs[i]);
        SingleLayerSeekExecutors &tmp = termQueryExecutors[i];
        for (uint32_t j = 0; j < tmp.size(); ++j) {
            delete tmp[j];
        }
    }
}

END_HA3_NAMESPACE(search);

