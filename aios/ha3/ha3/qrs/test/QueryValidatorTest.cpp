#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QueryValidator.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <ha3/common/ErrorDefine.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);

class QueryValidatorTest : public TESTBASE {
public:
    QueryValidatorTest();
    ~QueryValidatorTest();
public:
    void setUp();
    void tearDown();
protected:
    template <typename QueryType>
    void testIndexNotExist();

    template <typename QueryType>
    void testIndexExist();

protected:
    IndexInfos* createFakeIndexInfos();
protected:
    IndexInfos* _indexInfos;
    QueryValidator *_queryValidator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QueryValidatorTest);


QueryValidatorTest::QueryValidatorTest() { 
}

QueryValidatorTest::~QueryValidatorTest() { 
}

void QueryValidatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");

    _indexInfos = createFakeIndexInfos();
    _queryValidator = new QueryValidator(_indexInfos);
}

void QueryValidatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    
    DELETE_AND_SET_NULL(_indexInfos);
    DELETE_AND_SET_NULL(_queryValidator);
}

TEST_F(QueryValidatorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    RequiredFields requiredFields;
    QueryPtr queryPtr(new TermQuery("a", "not_exist_index", requiredFields, ""));
    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_INDEX_NOT_EXIST, _queryValidator->getErrorCode());
    string errorMsg = "indexName:[not_exist_index] not exist";
    ASSERT_EQ(errorMsg, _queryValidator->getErrorMsg());
}

TEST_F(QueryValidatorTest, testNullQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    QueryPtr queryPtr(new AndQuery(""));
    RequiredFields requiredFields;
    queryPtr->addQuery(QueryPtr(new TermQuery("a", "default", requiredFields, "")));
    queryPtr->addQuery(QueryPtr());
    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_NULL_QUERY, _queryValidator->getErrorCode());
}

TEST_F(QueryValidatorTest, testNullPhraseQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    QueryPtr queryPtr(new PhraseQuery(""));
    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_NULL_PHRASE_QUERY, _queryValidator->getErrorCode());
}

TEST_F(QueryValidatorTest, testNullMultiTermQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    QueryPtr queryPtr(new MultiTermQuery(""));
    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_NULL_MULTI_TERM_QUERY, _queryValidator->getErrorCode());
}

TEST_F(QueryValidatorTest, testOneNullMultiTermQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    MultiTermQuery *mQuery = new MultiTermQuery("");
    RequiredFields requiredFields;
    TermPtr tp1(new Term("", "default", requiredFields));
    TermPtr tp2(new Term("hello", "default", requiredFields));
    TermPtr tp3(new Term("", "default", requiredFields));
    mQuery->addTerm(tp1);
    mQuery->addTerm(tp2);
    mQuery->addTerm(tp3);
    QueryPtr queryPtr;
    queryPtr.reset(mQuery);
    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_NONE, _queryValidator->getErrorCode());
}

TEST_F(QueryValidatorTest, testIndexNotExistInPhraseQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    PhraseQuery *phraseQuery = new PhraseQuery("");
    RequiredFields requiredFields;
    phraseQuery->addTerm(TermPtr(new Term("a b c", "not_exist_index", 
                            requiredFields)));
    QueryPtr queryPtr(phraseQuery);

    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_INDEX_NOT_EXIST, _queryValidator->getErrorCode());
    string errorMsg = "indexName:[not_exist_index] not exist";
    ASSERT_EQ(errorMsg, _queryValidator->getErrorMsg());
}

TEST_F(QueryValidatorTest, testIndexNotExistInNumberQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    RequiredFields requiredFields;
    QueryPtr queryPtr(new NumberQuery(11, "not_exist_index", requiredFields, ""));
    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_INDEX_NOT_EXIST, _queryValidator->getErrorCode());
    string errorMsg = "indexName:[not_exist_index] not exist";
    ASSERT_EQ(errorMsg, _queryValidator->getErrorMsg());
}

TEST_F(QueryValidatorTest, testRangeErrorInNumberQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    RequiredFields requiredFields;
    QueryPtr queryPtr(new NumberQuery(11, true, 10, true, "default", requiredFields, ""));
    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_NUMBER_RANGE_ERROR, _queryValidator->getErrorCode());
    string errorMsg = "Number range invalid, leftNumber:[11], rightNumber:[10]";
    ASSERT_EQ(errorMsg, _queryValidator->getErrorMsg());
}

TEST_F(QueryValidatorTest, testIndexNotExistInAndQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    testIndexNotExist<AndQuery>();
}

TEST_F(QueryValidatorTest, testIndexNotExistInOrQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    testIndexNotExist<OrQuery>();
}

TEST_F(QueryValidatorTest, testIndexNotExistInRankQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    testIndexNotExist<RankQuery>();
}

TEST_F(QueryValidatorTest, testIndexNotExistInAndNotQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    testIndexNotExist<AndNotQuery>();
}


TEST_F(QueryValidatorTest, testIndexExistInTermQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    RequiredFields requiredFields;
    QueryPtr queryPtr(new TermQuery("a", "default", requiredFields, ""));
    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_NONE, _queryValidator->getErrorCode());
}

TEST_F(QueryValidatorTest, testTermQueryWithRequiredFields) {
    HA3_LOG(DEBUG, "Begin Test!");
    //success
    {
        RequiredFields requiredFields;
        requiredFields.fields.push_back("title");
        requiredFields.fields.push_back("body");
        QueryPtr queryPtr(new TermQuery("a", "expack_index", requiredFields, ""));
        queryPtr->accept(_queryValidator);
        ASSERT_EQ(ERROR_NONE, _queryValidator->getErrorCode());
    }
    //failed filedName not existed
    {
        RequiredFields requiredFields;
        requiredFields.fields.push_back("title");
        requiredFields.fields.push_back("not existed field");
        QueryPtr queryPtr(new TermQuery("a", "expack_index", requiredFields, ""));
        queryPtr->accept(_queryValidator);
        ASSERT_EQ(ERROR_INDEX_FIELD_INVALID,
                             _queryValidator->getErrorCode());
    }

    //failed indexType invalid
    {
        RequiredFields requiredFields;
        requiredFields.fields.push_back("title");
        requiredFields.fields.push_back("body");
        QueryPtr queryPtr(new TermQuery("a", "default", requiredFields, ""));
        queryPtr->accept(_queryValidator);
        ASSERT_EQ(ERROR_INDEX_TYPE_INVALID, 
                             _queryValidator->getErrorCode());
    }
    //failed filedName not existed and indexType invalid
    {
        RequiredFields requiredFields;
        requiredFields.fields.push_back("title");
        requiredFields.fields.push_back("not_exist");
        QueryPtr queryPtr(new TermQuery("a", "default", requiredFields, ""));
        queryPtr->accept(_queryValidator);
        ASSERT_EQ(ERROR_INDEX_TYPE_INVALID, 
                             _queryValidator->getErrorCode());
    }
}

TEST_F(QueryValidatorTest, testNumberQueryWithRequiredFields) {
    HA3_LOG(DEBUG, "Begin Test!");
    //success
    {
        RequiredFields requiredFields;
        requiredFields.fields.push_back("title");
        requiredFields.fields.push_back("body");
        QueryPtr queryPtr(new NumberQuery(11, "expack_index", requiredFields, ""));
        queryPtr->accept(_queryValidator);
        ASSERT_EQ(ERROR_NONE, _queryValidator->getErrorCode());
    }
}

TEST_F(QueryValidatorTest, testPhraseQueryWithRequiredFields) {
    HA3_LOG(DEBUG, "Begin Test!");
    //success
    {
        RequiredFields requiredFields;
        requiredFields.fields.push_back("title");
        requiredFields.fields.push_back("body");
        PhraseQuery *phraseQuery = new PhraseQuery("");
        phraseQuery->addTerm(TermPtr(new Term("a b c", "expack_index", 
                                requiredFields)));
        QueryPtr queryPtr(phraseQuery);
        queryPtr->accept(_queryValidator);
        ASSERT_EQ(ERROR_NONE, _queryValidator->getErrorCode());
    }
}


TEST_F(QueryValidatorTest, testIndexExistInPhraseQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    PhraseQuery *phraseQuery = new PhraseQuery("");
    RequiredFields requiredFields;
    phraseQuery->addTerm(TermPtr(new Term("a b c", "default", requiredFields)));
    QueryPtr queryPtr(phraseQuery);
    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_NONE, _queryValidator->getErrorCode());
}

TEST_F(QueryValidatorTest, testIndexExistInNumberQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_queryValidator);

    RequiredFields requiredFields;
    QueryPtr queryPtr(new NumberQuery(11, "default", requiredFields, ""));
    queryPtr->accept(_queryValidator);

    ASSERT_EQ(ERROR_NONE, _queryValidator->getErrorCode());
}

TEST_F(QueryValidatorTest, testIndexExistInAndQuery) {
   HA3_LOG(DEBUG, "Begin Test!");
   testIndexExist<AndQuery>();
}

TEST_F(QueryValidatorTest, testIndexExistInOrQuery) {
   HA3_LOG(DEBUG, "Begin Test!");
   testIndexExist<OrQuery>();
}

TEST_F(QueryValidatorTest, testIndexExistInAndNotQuery) {
   HA3_LOG(DEBUG, "Begin Test!");
   testIndexExist<AndNotQuery>();
}

TEST_F(QueryValidatorTest, testIndexExistInRankQuery) {
   HA3_LOG(DEBUG, "Begin Test!");
   testIndexExist<RankQuery>();
}

template <typename QueryType>
void QueryValidatorTest::testIndexNotExist() 
{
    assert(_queryValidator);

    QueryPtr queryPtr(new QueryType(""));
    RequiredFields requiredField;
    queryPtr->addQuery(QueryPtr(new TermQuery("a", "default", requiredField, "")));
    queryPtr->addQuery(QueryPtr(new TermQuery("b", "not_exist_index", requiredField, "")));
    queryPtr->accept(_queryValidator);

    assert(ERROR_INDEX_NOT_EXIST == _queryValidator->getErrorCode());
    string errorMsg = "indexName:[not_exist_index] not exist";
    assert(errorMsg == _queryValidator->getErrorMsg());
}

template <typename QueryType>
void QueryValidatorTest::testIndexExist() 
{
    assert(_queryValidator);

    QueryPtr queryPtr(new QueryType(""));
    RequiredFields requiredField;
    queryPtr->addQuery(QueryPtr(new TermQuery("a", "default", requiredField, "")));
    queryPtr->addQuery(QueryPtr(new TermQuery("b", "default", requiredField, "")));
    queryPtr->accept(_queryValidator);

    assert(ERROR_NONE == _queryValidator->getErrorCode());
}

IndexInfos* QueryValidatorTest::createFakeIndexInfos() {
    IndexInfos* indexInfos = new IndexInfos();
    IndexInfo* indexInfo = new IndexInfo();
    indexInfo->setIndexName("default");
    indexInfo->addField("title", 100);
    indexInfo->addField("body", 50);
    indexInfo->setIndexType(it_text);
    indexInfo->setAnalyzerName("SimpleAnalyzer");
    indexInfos->addIndexInfo(indexInfo);

    IndexInfo* indexInfo2 = new IndexInfo();
    indexInfo2->setIndexName("expack_index");
    indexInfo2->addField("title", 100);
    indexInfo2->addField("body", 50);
    indexInfo2->setIndexType(it_expack);
    indexInfo2->setAnalyzerName("SimpleAnalyzer");
    indexInfos->addIndexInfo(indexInfo2);
    return indexInfos;
}

END_HA3_NAMESPACE(qrs);

