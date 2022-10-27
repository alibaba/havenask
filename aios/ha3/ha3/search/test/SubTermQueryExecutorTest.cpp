#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SubTermQueryExecutor.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/test/QueryExecutorTestHelper.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);

class SubTermQueryExecutorTest : public TESTBASE {
public:
    SubTermQueryExecutorTest();
    ~SubTermQueryExecutorTest();
public:
    void setUp();
    void tearDown();
protected:
    QueryExecutorPtr createSubTermQueryExecutor(
            const index::FakeIndex& index);
    QueryExecutorPtr createSubTermQueryExecutor(
            const std::string &subDocIndex,
            const std::string& mainToSub, 
            const std::string& subToMain);
    QueryExecutorPtr createSubTermQueryExecutor(
            const std::string &subDocIndex,
            const std::string &subDocAttrMap);
protected:
    IndexPartitionReaderWrapperPtr _indexPartReader;
    autil::mem_pool::Pool _pool;
    AttributeIteratorBase* _mainDocToSubDocIt;
    AttributeIteratorBase* _subDocToMainDocIt;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, SubTermQueryExecutorTest);


SubTermQueryExecutorTest::SubTermQueryExecutorTest() { 
}

SubTermQueryExecutorTest::~SubTermQueryExecutorTest() { 
}

void SubTermQueryExecutorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _mainDocToSubDocIt = NULL;
    _subDocToMainDocIt = NULL;
}

void SubTermQueryExecutorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SubTermQueryExecutorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    QueryExecutorPtr queryExecutor = createSubTermQueryExecutor("0;2;6;7", "3,5,10");
    ASSERT_TRUE(queryExecutor->hasSubDocExecutor());
    ASSERT_EQ(docid_t(0), queryExecutor->legacySeek(0));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(0));
    ASSERT_EQ(docid_t(0), queryExecutor->legacySeekSub(0, 0, 4));
    ASSERT_EQ(docid_t(2), queryExecutor->legacySeekSub(0, 1, 4));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(0, 3, 4));
    ASSERT_EQ(docid_t(2), queryExecutor->legacySeek(1));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(1, 4, 5));
    ASSERT_EQ(docid_t(6), queryExecutor->legacySeekSub(2, 6, 10));
    ASSERT_EQ(docid_t(7), queryExecutor->legacySeekSub(2, 7, 10));
    ASSERT_EQ(END_DOCID, queryExecutor->legacySeekSub(2, 8, 10));
    POOL_DELETE_CLASS(_mainDocToSubDocIt);
    POOL_DELETE_CLASS(_subDocToMainDocIt);
}

TEST_F(SubTermQueryExecutorTest, testSeekWhenNotSetMainJoin) { 
    HA3_LOG(DEBUG, "Begin Test!");

    QueryExecutorPtr queryExecutor = createSubTermQueryExecutor(
            "0;2;", "2,-1", "0,0,1,1");
    ASSERT_TRUE(queryExecutor->hasSubDocExecutor());
    ASSERT_EQ(docid_t(0), queryExecutor->legacySeek(0));
    ASSERT_EQ(docid_t(1), queryExecutor->legacySeek(1));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeek(2));
    POOL_DELETE_CLASS(_mainDocToSubDocIt);
    POOL_DELETE_CLASS(_subDocToMainDocIt);
}


TEST_F(SubTermQueryExecutorTest, testMainDeleted) { 
    {
        // no more doc, seek to end
        QueryExecutorPtr queryExecutor = createSubTermQueryExecutor("0;2;6;7", "3,5,10");
        ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeek(3));
        POOL_DELETE_CLASS(_mainDocToSubDocIt);
        POOL_DELETE_CLASS(_subDocToMainDocIt);
    }
    {
        // previous doc deleted
        int subToMainArr[] = { 0, 0, 0, 
                               INVALID_DOCID, INVALID_DOCID,
                               2, 2, 2, 2, 2 };
        vector<int> subDocToMainDoc(subToMainArr, subToMainArr + 9);
        string subToMain = StringUtil::toString(subDocToMainDoc, ",");
        QueryExecutorPtr queryExecutor = createSubTermQueryExecutor(
                "0;2;3;4;5;6;7", 
                "3,3,10",
                subToMain);
        ASSERT_EQ(docid_t(0), queryExecutor->legacySeek(0));
        ASSERT_EQ(docid_t(0), queryExecutor->legacySeekSub(0, 0, 3));
        ASSERT_EQ(docid_t(2), queryExecutor->legacySeekSub(0, 1, 3));
        ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(0, 3, 3));
        ASSERT_EQ(docid_t(2), queryExecutor->legacySeek(1));
        ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(1, 4, 5));
        ASSERT_EQ(docid_t(6), queryExecutor->legacySeekSub(2, 6, 10));
        ASSERT_EQ(docid_t(7), queryExecutor->legacySeekSub(2, 7, 10));
        ASSERT_EQ(END_DOCID, queryExecutor->legacySeekSub(2, 8, 10));
        POOL_DELETE_CLASS(_mainDocToSubDocIt);
        POOL_DELETE_CLASS(_subDocToMainDocIt);
    }
    {
        // all previous doc deleted
        int subToMainArr[] = { INVALID_DOCID, INVALID_DOCID, INVALID_DOCID, 
                               INVALID_DOCID, INVALID_DOCID,
                               2, 2, 2, 2, 2 };
        vector<int> subDocToMainDoc(subToMainArr, subToMainArr + 9);
        string subToMain = StringUtil::toString(subDocToMainDoc, ",");
        QueryExecutorPtr queryExecutor = createSubTermQueryExecutor(
                "0;2;3;4;5;6;7", 
                "0,0,10",
                subToMain);
        ASSERT_EQ(docid_t(2), queryExecutor->legacySeek(0));
        ASSERT_EQ(docid_t(END_DOCID), 
                             queryExecutor->legacySeekSub(0, 0, 3));
        ASSERT_EQ(docid_t(END_DOCID), 
                             queryExecutor->legacySeekSub(0, 1, 3));
        ASSERT_EQ(docid_t(END_DOCID), 
                             queryExecutor->legacySeekSub(0, 3, 3));
        ASSERT_EQ(docid_t(2), queryExecutor->legacySeek(1));
        ASSERT_EQ(docid_t(END_DOCID), 
                             queryExecutor->legacySeekSub(1, 4, 5));
        ASSERT_EQ(docid_t(6), queryExecutor->legacySeekSub(2, 6, 10));
        ASSERT_EQ(docid_t(7), queryExecutor->legacySeekSub(2, 7, 10));
        ASSERT_EQ(END_DOCID, queryExecutor->legacySeekSub(2, 8, 10));
        POOL_DELETE_CLASS(_mainDocToSubDocIt);
        POOL_DELETE_CLASS(_subDocToMainDocIt);
    }
}

QueryExecutorPtr SubTermQueryExecutorTest::createSubTermQueryExecutor(
        const index::FakeIndex& index)
{
    _indexPartReader = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(index);
    Term term("term", "sub_index_name", RequiredFields());
    LookupResult result = _indexPartReader->lookup(term, NULL);
    PostingIterator* posting = result.postingIt;

    const IndexPartitionReaderPtr &readerPtr = _indexPartReader->getReader();
    AttributeReaderPtr attrReader = readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    _mainDocToSubDocIt = attrReader->CreateIterator(&_pool);
    SubTermQueryExecutor::DocMapAttrIterator *typedMainDocToSubDocIt = 
        dynamic_cast<SubTermQueryExecutor::DocMapAttrIterator*>(_mainDocToSubDocIt);

    attrReader = readerPtr->GetAttributeReader(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);
    _subDocToMainDocIt = attrReader->CreateIterator(&_pool);
    SubTermQueryExecutor::DocMapAttrIterator *typedSubDocToMainDocIt = 
        dynamic_cast<SubTermQueryExecutor::DocMapAttrIterator*>(_subDocToMainDocIt);
    return QueryExecutorPtr(new SubTermQueryExecutor(
                    posting, term, typedMainDocToSubDocIt, typedSubDocToMainDocIt));
}

QueryExecutorPtr SubTermQueryExecutorTest::createSubTermQueryExecutor(
        const string &subDocIndex,
        const string& mainToSub, 
        const string& subToMain)
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["sub_index_name"] = "term:" + subDocIndex + ";\n";
    fakeIndex.attributes += MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME": int32_t : "
                            + mainToSub + ";\n"
                            SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME": int32_t : "
                            + subToMain + ";\n";
    return createSubTermQueryExecutor(fakeIndex);
}

QueryExecutorPtr SubTermQueryExecutorTest::createSubTermQueryExecutor(
        const string &subDocIndex,
        const string &subDocAttrMap)
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["sub_index_name"] = "term:" + subDocIndex + ";\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, subDocAttrMap);
    return createSubTermQueryExecutor(fakeIndex);
}

END_HA3_NAMESPACE(search);

