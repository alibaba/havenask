#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/ProviderBase.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;
USE_HA3_NAMESPACE(common);
IE_NAMESPACE_USE(index);

BEGIN_HA3_NAMESPACE(search);

class ProviderBaseTest : public TESTBASE {
public:
    ProviderBaseTest();
    ~ProviderBaseTest();
public:
    void setUp();
    void tearDown();
protected:
    void innerTestRequireMatchData(SubDocDisplayType displayType, bool simple, bool support);
    void innerTestRequireMatchValues(SubDocDisplayType displayType, bool support);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, ProviderBaseTest);


ProviderBaseTest::ProviderBaseTest() { 
}

ProviderBaseTest::~ProviderBaseTest() { 
}

void ProviderBaseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ProviderBaseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ProviderBaseTest, testRequireMatchData) { 
    HA3_LOG(DEBUG, "Begin Test!");

    innerTestRequireMatchData(SUB_DOC_DISPLAY_NO, true, true);
    innerTestRequireMatchData(SUB_DOC_DISPLAY_FLAT, true, false);
    innerTestRequireMatchData(SUB_DOC_DISPLAY_GROUP, true, true);

    innerTestRequireMatchData(SUB_DOC_DISPLAY_NO, false, true);
    innerTestRequireMatchData(SUB_DOC_DISPLAY_FLAT, false, false);
    innerTestRequireMatchData(SUB_DOC_DISPLAY_GROUP, false, true);
}

TEST_F(ProviderBaseTest, testRequireMatchValues) { 
    HA3_LOG(DEBUG, "Begin Test!");

    innerTestRequireMatchValues(SUB_DOC_DISPLAY_NO, true);
    innerTestRequireMatchValues(SUB_DOC_DISPLAY_FLAT, false);
    innerTestRequireMatchValues(SUB_DOC_DISPLAY_GROUP, true);
}

void ProviderBaseTest::innerTestRequireMatchData(SubDocDisplayType displayType, bool simple, bool support) {
    autil::mem_pool::Pool *pool = new autil::mem_pool::Pool;

    Request *request = new Request;
    ConfigClause *configClause = new ConfigClause();
    configClause->setSubDocDisplayType(displayType);
    request->setConfigClause(configClause);

    MatchDataManager *manager = new MatchDataManager;

    FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:1;2;3;4\n"
                                   "b:1;3;4\n"
                                   "c:2;3;5\n";
    string queryStr = "a OR b OR c";
    IndexPartitionReaderWrapperPtr indexPartReaderPtr = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    indexPartReaderPtr->setTopK(1);
    unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));
    QueryExecutorCreator *creator = new QueryExecutorCreator(manager, indexPartReaderPtr.get(), pool);
    query->accept(creator);

    SearchPluginResource resource;
    resource.pool = pool;
    resource.matchDataManager = manager;
    resource.request = request;
    resource.matchDocAllocator.reset(new common::Ha3MatchDocAllocator(pool));

    ProviderBase provider;
    provider.setSearchPluginResource(&resource);

    if (simple) {
        ASSERT_EQ(support, provider.requireSimpleMatchData() != NULL);
    } else {
        ASSERT_EQ(support, provider.requireMatchData() != NULL);
    }

    delete request;
    delete creator;
    delete manager;
    query.reset();
    resource.matchDocAllocator.reset();
    delete pool;
}

void ProviderBaseTest::innerTestRequireMatchValues(SubDocDisplayType displayType, bool support) {
    autil::mem_pool::Pool *pool = new autil::mem_pool::Pool;

    Request *request = new Request;
    ConfigClause *configClause = new ConfigClause();
    configClause->setSubDocDisplayType(displayType);
    request->setConfigClause(configClause);

    MatchDataManager *manager = new MatchDataManager;

    FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:1;2;3;4\n"
                                   "b:1;3;4\n"
                                   "c:2;3;5\n";
    string queryStr = "a OR b OR c";
    IndexPartitionReaderWrapperPtr indexPartReaderPtr = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    indexPartReaderPtr->setTopK(1);
    unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));
    QueryExecutorCreator *creator = new QueryExecutorCreator(manager, indexPartReaderPtr.get(), pool);
    query->accept(creator);

    SearchPluginResource resource;
    resource.pool = pool;
    resource.matchDataManager = manager;
    resource.request = request;
    resource.matchDocAllocator.reset(new common::Ha3MatchDocAllocator(pool));

    ProviderBase provider;
    provider.setSearchPluginResource(&resource);

    ASSERT_EQ(support, provider.requireMatchValues() != NULL);

    delete request;
    delete creator;
    delete manager;
    query.reset();
    resource.matchDocAllocator.reset();
    delete pool;
}

END_HA3_NAMESPACE(search);

