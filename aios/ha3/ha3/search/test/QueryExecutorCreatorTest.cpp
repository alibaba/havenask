#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3_sdk/testlib/index/FakePostingMaker.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3_sdk/testlib/index/IndexPartitionReaderWrapperCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexReader.h>
#include <ha3/common/Term.h>
#include <ha3/common/TermQuery.h>
#include <ha3/search/TermQueryExecutor.h>
#include <autil/StringTokenizer.h>
#include <ha3/search/test/QueryExecutorTestHelper.h>
#include <indexlib/testlib/schema_maker.h>
using namespace std;
using namespace std::tr1;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(rank);
BEGIN_HA3_NAMESPACE(search);

class QueryExecutorCreatorTest : public TESTBASE {
public:
    QueryExecutorCreatorTest();
    ~QueryExecutorCreatorTest();
public:
    void setUp();
    void tearDown();
protected:
    void setPostingType(const std::string &term,
                        PostingIteratorType type,
                        const index::IndexReaderPtr &readerPtr);
    void creatorTestInternal(const IndexPartitionReaderWrapperPtr& reader,
                             const std::string &queryStr,
                             const std::string &exceptQueryStr,
                             const std::string &unpackTerms,
                             bool movedToEnd = false, bool hasSubDocExecutor = false);
    void creatorTestInternal(const index::FakeIndex &fakeIndex,
                             const std::string &queryStr,
                             const std::string &exceptQueryStr,
                             const std::string &unpackTerms,
                             bool movedToEnd = false, bool hasSubDocExecutor = false);
    void checkUnpackTerms(MatchDataManager &matchDataManager, const std::string &unpackTerms);
    void createIndexPartitionReaderWrapper(const std::string &indexStr);
    index::IndexReaderPtr getFakeTextIndexReader();
    void expectSeekResult(
            const Query &query,
            IndexPartitionReaderWrapperPtr indexPartReaderPtr,
            const vector<docid_t> &docids);
    template<typename T>
    void singleColumnTableQueryTest(
            IndexPartitionReaderWrapperPtr indexPartReaderPtr,
            const string &indexName,
            const vector<T> &values,
            const vector<size_t> &offsets,
            const vector<docid_t> &docids,
            bool enableCache);
    template<typename T>
    void MultiColumnsTableQueryTest(
            IndexPartitionReaderWrapperPtr indexPartReaderPtr,
            const vector<string> &indexNames,
            const vector<vector<T>> &values,
            const vector<vector<size_t>> &offsets,
            const vector<docid_t> &docids);
    void checkTableQuery(IndexPartitionReaderWrapperPtr indexPartReaderPtr, bool enableCache);
protected:
    MatchDataManager *_matchDataManager;
    QueryExecutorCreator *_qeCreator;
    autil::mem_pool::Pool *_pool;
    IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, QueryExecutorCreatorTest);

QueryExecutorCreatorTest::QueryExecutorCreatorTest() {
    _matchDataManager = NULL;
    _qeCreator = NULL;
    _pool = new autil::mem_pool::Pool(1024);
}

QueryExecutorCreatorTest::~QueryExecutorCreatorTest() {
    DELETE_AND_SET_NULL(_qeCreator);
    DELETE_AND_SET_NULL(_matchDataManager);
    delete _pool;
}

void QueryExecutorCreatorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _matchDataManager = new MatchDataManager;
    _matchDataManager->setQueryCount(1);
}

void QueryExecutorCreatorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_qeCreator);
    DELETE_AND_SET_NULL(_matchDataManager);
}

TEST_F(QueryExecutorCreatorTest, testVisitTermQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:3[1];";
    createIndexPartitionReaderWrapper(one);
    _qeCreator = new QueryExecutorCreator(_matchDataManager,
            _indexPartitionReaderWrapper.get(), _pool);
    RequiredFields requiredFields;
    Term term("one", "indexname", requiredFields);
    TermQuery termQuery(term, "");
    _qeCreator->visitTermQuery(&termQuery);
    QueryExecutor *queryExecutor = _qeCreator->stealQuery();
    ASSERT_TRUE(queryExecutor);
    ASSERT_EQ(df_t(1), queryExecutor->getCurrentDF());
    POOL_DELETE_CLASS(queryExecutor);
    ASSERT_EQ((int)_matchDataManager->getQueryNum(), 1);
}

TEST_F(QueryExecutorCreatorTest, testVisitTermQueryNoMatchdata) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:3[1];";
    createIndexPartitionReaderWrapper(one);
    _qeCreator = new QueryExecutorCreator(_matchDataManager,
            _indexPartitionReaderWrapper.get(), _pool);
    RequiredFields requiredFields;
    Term term("one", "indexname", requiredFields);
    TermQuery termQuery(term, "");
    termQuery.setMatchDataLevel(MDL_NONE);
    _qeCreator->visitTermQuery(&termQuery);
    QueryExecutor *queryExecutor = _qeCreator->stealQuery();
    ASSERT_TRUE(queryExecutor);
    ASSERT_EQ(df_t(1), queryExecutor->getCurrentDF());
    POOL_DELETE_CLASS(queryExecutor);
    ASSERT_EQ((int)_matchDataManager->getQueryNum(), 0);
}

TEST_F(QueryExecutorCreatorTest, testSubQueryMatchdata) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:3[1];";
    createIndexPartitionReaderWrapper(one);
    _qeCreator = new QueryExecutorCreator(_matchDataManager,
            _indexPartitionReaderWrapper.get(), _pool);
    RequiredFields requiredFields;
    Term term("one", "indexname", requiredFields);
    TermQueryPtr termQuery(new TermQuery(term, ""));
    termQuery->setMatchDataLevel(MDL_NONE);
    OrQuery orQuery("");
    orQuery.setMatchDataLevel(MDL_SUB_QUERY);
    orQuery.setQueryLabel("hihi");
    orQuery.addQuery(termQuery);

    _qeCreator->visitOrQuery(&orQuery);
    QueryExecutor *queryExecutor = _qeCreator->stealQuery();
    ASSERT_TRUE(queryExecutor);
    POOL_DELETE_CLASS(queryExecutor);
    ASSERT_EQ((int)_matchDataManager->getQueryNum(), 1);

    MetaInfo metaInfo;
    _matchDataManager->getQueryTermMetaInfo(&metaInfo);
    ASSERT_EQ((int)metaInfo.size(), 1);
    ASSERT_EQ(metaInfo[0].getMatchDataLevel(), MDL_SUB_QUERY);
    ASSERT_EQ(metaInfo[0].getQueryLabel(), std::string("hihi"));
}

TEST_F(QueryExecutorCreatorTest, testVisitTermQueryNoResult) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:3[1];";
    createIndexPartitionReaderWrapper(one);
    _qeCreator = new QueryExecutorCreator(_matchDataManager,
            _indexPartitionReaderWrapper.get(), _pool);

    RequiredFields requiredFields;
    Term term("nosuchterm", "indexname", requiredFields);
    TermQuery termQuery(term, "");
    _qeCreator->visitTermQuery(&termQuery);
    QueryExecutor *queryExecutor = _qeCreator->stealQuery();
    ASSERT_TRUE(queryExecutor);
    ASSERT_TRUE(queryExecutor->isEmpty());
    POOL_DELETE_CLASS(queryExecutor);
    ASSERT_EQ((int)_matchDataManager->getQueryNum(), 1);
}

TEST_F(QueryExecutorCreatorTest, testQueryExecutorGetDF) {
    string postingStr = "one:3;4;5;6;\n"
                        "two:3;\n";
    createIndexPartitionReaderWrapper(postingStr);

    _qeCreator = new QueryExecutorCreator(_matchDataManager,
            _indexPartitionReaderWrapper.get(), _pool);
    unique_ptr<Query> query(SearcherTestHelper::createQuery("one ANDNOT two", 0, "indexname"));
    query->accept(_qeCreator);
    QueryExecutor *queryExecutor = _qeCreator->stealQuery();
    ASSERT_EQ(df_t(4), queryExecutor->getCurrentDF());
    POOL_DELETE_CLASS(queryExecutor);
}

void QueryExecutorCreatorTest::setPostingType(const string &term,
        PostingIteratorType type, const IndexReaderPtr &readerPtr)
{
    FakeTextIndexReaderPtr reader = dynamic_pointer_cast<FakeTextIndexReader>(
            readerPtr);
    ASSERT_TRUE(reader);
    reader->setType(term, type);
}


TEST_F(QueryExecutorCreatorTest, testAndQueryCreate) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "a AND b";
        string expectQueryStr = "AND(a,b)";
        string unpackTerms = "a,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }

    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3;4\n"
                                       "c:1\n";
        string queryStr = "a AND b AND c";
        string expectQueryStr = "AND(c,a,b)";
        string unpackTerms = "a, b, c";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;2\n";
        string queryStr = "b AND a";
        string unpackTerms = "a, b";
        creatorTestInternal(fakeIndex, queryStr, "", "");
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;2\n"
                                       "b:1;3\n";
        string queryStr = "a AND c OR b ";
        string expectQueryStr = "OR(AND(c,a),b)";
        string unpackTerms = "a,c,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
}

TEST_F(QueryExecutorCreatorTest, testMultiTermQueryCreate) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "default:'a' | 'b'";
        string expectQueryStr = "MultiTermOr(a,b)";
        string unpackTerms = "a,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }

    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3;4\n"
                                       "c:1\n";
        string queryStr = "default:'a' & 'b' & 'c'";
        string expectQueryStr = "MultiTermAnd(a,b,c)";
        string unpackTerms = "a, b, c";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }

    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "c:1\n";
        string queryStr = "default:'a' | 'b' | 'c'";
        string expectQueryStr = "MultiTermOr(a,b,c)";
        string unpackTerms = "a, b, c";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }

    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3;4\n"
                                       "c:1\n";
        string queryStr = "default:'a' & 'b' & 'd'";
        creatorTestInternal(fakeIndex, queryStr, "", "");
    }

    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;2\n";
        string queryStr = "default:'b' & 'a'";
        creatorTestInternal(fakeIndex, queryStr, "", "");
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;2\n"
                                       "b:1;3\n";
        string queryStr = "default:'a' & 'c' | 'b'";
        string expectQueryStr = "OR(MultiTermAnd(a,c),b)";
        string unpackTerms = "a,c,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
}

TEST_F(QueryExecutorCreatorTest, testFieldMapQueryExecutor) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["expack_index"] = "a:1\n"
                                            "b:2\n";
        string queryStr = "expack_index[field0,field1]:a AND expack_index(field1,field5):b";
        string expectQueryStr = "AND(expack_index[3]:a,expack_index(34):b)";
        string unpackTerms = "a,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["expack_index"] = "a:1\n"
                                            "b:2\n";
        string queryStr = "expack_index(field1,field5):c";
        string expectQueryStr = "";
        string unpackTerms = "c";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["expack_index"] = "a:1\n"
                                            "b:2\n";
        string queryStr = "expack_index(field1,field5):\"a b\"";
        string expectQueryStr = "PHRASE(expack_index(34):a,expack_index(34):b)";
        string unpackTerms = "a,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["expack_index"] = "a:1\n"
                                            "b:2\n";
        fakeIndex.indexes["bitmap_expack_index"] = "a:1\n"
                                                   "b:2\n";
        string queryStr = "expack_index(field1,field5):b#BITMAP";
        string expectQueryStr = "b";
        string unpackTerms = "b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
}

TEST_F(QueryExecutorCreatorTest, testSpatialShapeQuery) {
    string field = "line:line;polygon:polygon";
    string index = "line_index:spatial:line;polygon_index:spatial:polygon";
    string attribute = "line;polygon";
    string summary = "";

    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema(
            "table", field, index, attribute, "");
    ASSERT_TRUE(schema);
    string docString = "cmd=add|pk=1|polygon="
                       "0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1,0.1 30.1|"
                       "line=0.1 30.1,30.1 30.1";
    IndexPartitionPtr indexPartition =
        IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                schema, GET_TEMPLATE_DATA_PATH(), docString);
    vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(indexPartition);
    ReaderWrapperContainer container;
    IndexPartitionReaderWrapperCreator::CreateIndexPartitionReaderWrapper(
            indexPartitions, container);
    string queryStr =
        "polygon_index:\'polygon(0.0 40.0,40.0 40.0,40.0 0.0,0.0 0.0,0 40.0)\'";
    string expectQueryStr = queryStr;
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        container.readerWrapper;
    indexPartReaderPtr->setTopK(1000);
    MatchDataManager matchDataManager;
    matchDataManager.setQueryCount(1);
    QueryExecutorCreator creator(&matchDataManager, indexPartReaderPtr.get(), _pool);

    unique_ptr<Query> query(SearcherTestHelper::createQuery(
                        queryStr, 0, "default", false));
    query->accept(&creator);
    QueryExecutor *queryExecutor = creator.stealQuery();
    docid_t seekDoc = queryExecutor->legacySeek(0);
    ASSERT_EQ((docid_t)0, seekDoc);
    seekDoc = queryExecutor->legacySeek(seekDoc + 1);
    ASSERT_EQ(END_DOCID, seekDoc);
    POOL_DELETE_CLASS(queryExecutor);
}

TEST_F(QueryExecutorCreatorTest, testSpatialAndQueryExecutor) {
    string field = "location:location;number:int64:false";
    string index = "number_index:range:number;spatial_index:spatial:location";
    string attribute = "location;number";
    string summary = "";

    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema("table", field, index, attribute, "");
    string docString = "cmd=add,number=1,location=116.3906 39.92324116.3906 39.92325;"
                       "cmd=add,number=2,location=116.3906 39.92324;"
                       "cmd=add,number=3,location=116.3906 39.92324;"
                       "cmd=add,number=4,location=116.3906 39.92325;"
                       "cmd=add,number=5,location=116.3906 39.92335;"
                       "cmd=add,number=6,location=116.3906 39.92435;";

    IndexPartitionPtr indexPartition =
        IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                schema, GET_TEMPLATE_DATA_PATH(), docString);
    vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(indexPartition);
    ReaderWrapperContainer container;
    IndexPartitionReaderWrapperCreator::CreateIndexPartitionReaderWrapper(
            indexPartitions, container);
    {
        string queryStr = "(number_index:[1,4]) AND (spatial_index:\'rectangle(116.00 39.00, 117.00 40.00)\')";
        string expectQueryStr = queryStr;
        IndexPartitionReaderWrapperPtr indexPartReaderPtr =
            container.readerWrapper;
        indexPartReaderPtr->setTopK(1000);
        MatchDataManager matchDataManager;
        matchDataManager.setQueryCount(1);
        QueryExecutorCreator creator(&matchDataManager, indexPartReaderPtr.get(), _pool);

        unique_ptr<Query> query(SearcherTestHelper::createQuery(
                        queryStr, 0, "default", false));
        query->accept(&creator);
        QueryExecutor *queryExecutor = creator.stealQuery();
        docid_t seekDoc = queryExecutor->legacySeek(0);
        ASSERT_EQ((docid_t)0, seekDoc);
        seekDoc = queryExecutor->legacySeek(seekDoc + 1);
        ASSERT_EQ((docid_t)1, seekDoc);
        seekDoc = queryExecutor->legacySeek(seekDoc + 1);
        ASSERT_EQ((docid_t)2, seekDoc);
        seekDoc = queryExecutor->legacySeek(seekDoc + 1);
        ASSERT_EQ((docid_t)3, seekDoc);
        seekDoc = queryExecutor->legacySeek(seekDoc + 1);
        ASSERT_EQ(END_DOCID, seekDoc);
        //range iterator: 8 + spatial filter 4
        ASSERT_EQ(12u, queryExecutor->getSeekDocCount());
        POOL_DELETE_CLASS(queryExecutor);
    }
}

TEST_F(QueryExecutorCreatorTest, testNumberExecutor) {
    string field = "number:int64:false";
    string index = "number_index:range:number";
    string attribute = "number";
    string summary = "";

    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema("table", field, index, attribute, "");
    string docString = "cmd=add,number=1;"
                       "cmd=add,number=2;"
                       "cmd=add,number=3;"
                       "cmd=add,number=4;"
                       "cmd=add,number=5;"
                       "cmd=add,number=6;";

    IndexPartitionPtr indexPartition =
        IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                schema, GET_TEMPLATE_DATA_PATH(), docString);
    vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(indexPartition);
    ReaderWrapperContainer container;
    IndexPartitionReaderWrapperCreator::CreateIndexPartitionReaderWrapper(
            indexPartitions, container);
    {
        string queryStr = "number_index:[1,4]";
        string expectQueryStr = queryStr;
        IndexPartitionReaderWrapperPtr indexPartReaderPtr =
            container.readerWrapper;
        indexPartReaderPtr->setTopK(1000);
        MatchDataManager matchDataManager;
        matchDataManager.setQueryCount(1);
        QueryExecutorCreator creator(&matchDataManager, indexPartReaderPtr.get(), _pool);

        unique_ptr<Query> query(SearcherTestHelper::createQuery(
                        queryStr, 0, "default", false));
        query->accept(&creator);
        QueryExecutor *queryExecutor = creator.stealQuery();
        docid_t seekDoc = queryExecutor->legacySeek(0);
        ASSERT_EQ((docid_t)0, seekDoc);
        seekDoc = queryExecutor->legacySeek(seekDoc + 1);
        ASSERT_EQ((docid_t)1, seekDoc);
        seekDoc = queryExecutor->legacySeek(seekDoc + 1);
        ASSERT_EQ((docid_t)2, seekDoc);
        seekDoc = queryExecutor->legacySeek(seekDoc + 1);
        ASSERT_EQ((docid_t)3, seekDoc);
        seekDoc = queryExecutor->legacySeek(seekDoc + 1);
        ASSERT_EQ(END_DOCID, seekDoc);
        ASSERT_EQ(8u, queryExecutor->getSeekDocCount());
        queryExecutor->reset();
        POOL_DELETE_CLASS(queryExecutor);
    }
}

TEST_F(QueryExecutorCreatorTest, testNestAndQueryExecutor) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;2\n"
                                       "b:1;3\n"
                                       "c:2,3\n"
                                       "d:2,4\n";
        string queryStr = "((a ANDNOT b) AND e AND c) OR d ";
        string expectQueryStr = "OR(AND(e,c,ANDNOT(a,b)),d)";
        string unpackTerms = "a,e,c,d";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;2\n"
                                       "b:1;3\n";
        string queryStr = "a AND (e RANK b) ";
        string expectQueryStr = "AND(e,a)";
        string unpackTerms = "a,e,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;2\n"
                                       "b:1;3;5\n"
                                       "c:3;4;5;6\n";
        string queryStr = "a AND (b RANK d) AND c ";
        string expectQueryStr = "AND(a,b,c)";
        string unpackTerms = "a,b,d,c";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;2\n"
                                       "b:1;3\n"
                                       "c:3;4\n";
        string queryStr = "a AND (b RANK c) AND d ";
        string expectQueryStr = "";
        string unpackTerms = "";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1[1];2[1]\n"
                                       "b:1[1];3[3]\n"
                                       "c:3[3];4[5]\n";
        string queryStr = "a AND (\"b c\") AND d ";
        string expectQueryStr = "";
        string unpackTerms = "";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1[1];2[1]\n"
                                       "b:1[1];3[3]\n"
                                       "c:3[3];4[5]\n";
        string queryStr = "a AND (\"b d\") AND c";
        string expectQueryStr = "";
        string unpackTerms = "";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1[1];2[1]\n"
                                       "b:1[1];3[3]\n"
                                       "c:3[3];4[5]\n";
        string queryStr = "(\"a c\") AND (\"b d\")";
        string expectQueryStr = "";
        string unpackTerms = "";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1[1];2[1]\n"
                                       "b:1[1];3[3];6[3]\n"
                                       "c:3[3];4[5];5[3];7[3]\n";
        string queryStr = "(\"a c\") OR (\"b d\")";
        string expectQueryStr = "OR(PHRASE(a,c),PHRASE(b,d))";
        string unpackTerms = "a,c,b,d";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1[1];2[1]\n"
                                       "b:1[1];3[3];6[3]\n"
                                       "c:3[3];4[5];5[3];7[3]\n"
                                       "d:3[3];4[5];5[3];7[3];9[1]\n";
        string queryStr = "(\"a c\") AND (a OR (c AND(d AND (c AND e))) OR (a AND (c OR j)) )";
        string expectQueryStr = "AND(PHRASE(a,c),OR(a,AND(e,c,c,d),AND(a,OR(c,j))))";
        string unpackTerms = "a,c,a,c,d,c,e,a,c,j";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
}

TEST_F(QueryExecutorCreatorTest, testOrQueryCreate) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "a OR b";
        string expectQueryStr = "OR(a,b)";
        string unpackTerms = "a,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }

    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "a OR c OR b OR d";
        string expectQueryStr = "OR(a,c,b,d)";
        string unpackTerms = "a,c,b,d";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }

    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "c OR b OR d";
        string expectQueryStr = "OR(c,b,d)";
        string unpackTerms = "c,b,d";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
}


TEST_F(QueryExecutorCreatorTest, testRankQueryCreate) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "a RANK b";
        string expectQueryStr = "a";
        string unpackTerms = "a,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "a RANK b RANK c";
        string expectQueryStr = "a";
        string unpackTerms = "a,b,c";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n";
        string queryStr = "a RANK c";
        string expectQueryStr = "a";
        string unpackTerms = "a,c";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n";
        string queryStr = "c RANK a";
        string expectQueryStr = "c";
        string unpackTerms = "c,a";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
}

TEST_F(QueryExecutorCreatorTest, testPhraseQueryCreate) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1[2,3];3[5,6]\n"
                                       "b:2[3,5];3[3,4]\n";
        string queryStr = "\"a b\"";
        string expectQueryStr = "PHRASE(a,b)";
        string unpackTerms = "a,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1[2,3];3[5,6]\n"
                                       "b:2[3,5];3[3,4]\n"
                                       "c:1[2,3];3[5,6]\n"
                                       "d:1[2,3];3[5,6]\n";
        string queryStr = "\"a b c d\"";
        string expectQueryStr = "PHRASE(a,b,c,d)";
        string unpackTerms = "a,b,c,d";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1[2,3];3[5,6]\n"
                                       "b:2[3,5];3[3,4]\n";
        string queryStr = "\"a c\"";
        string expectQueryStr = "";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, "");
    }
}


TEST_F(QueryExecutorCreatorTest, testAndNotQueryCreate) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "a ANDNOT b";
        string expectQueryStr = "ANDNOT(a,b)";
        string unpackTerms = "a";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n"
                                       "c:33\n"
                                       "d:1\n";
        string queryStr = "a ANDNOT b ANDNOT c ANDNOT d";
        string expectQueryStr = "ANDNOT(a,OR(b,c,d))";
        string unpackTerms = "a";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n"
                                       "d:1\n";
        string queryStr = "a ANDNOT b ANDNOT c ANDNOT d";
        string expectQueryStr = "ANDNOT(a,OR(b,c,d))";
        string unpackTerms = "a";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n";
        string queryStr = "a ANDNOT b";
        string expectQueryStr = "ANDNOT(a,b)";
        string unpackTerms = "a";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n";
        string queryStr = "b ANDNOT a";
        string expectQueryStr = "ANDNOT(b,a)";
        string unpackTerms = "b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:1;4\n";
        string queryStr = "b ANDNOT a";
        string expectQueryStr = "ANDNOT(b,a)";
        string unpackTerms = "b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
}

TEST_F(QueryExecutorCreatorTest, testBitmapAndQueryCreate) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n";
        fakeIndex.indexes["bitmap1"] = "b:1;3\n";
        fakeIndex.indexes["bitmap2"] = "c:1;3\n";
        string queryStr = "default:a AND bitmap1:b AND bitmap2:c";
        string expectQueryStr = "BITMAPAND(a,TEST(b,c))";
        string unpackTerms = "a,b,c";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
}

TEST_F(QueryExecutorCreatorTest, testBitmapTruncateName) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n";
        fakeIndex.indexes["bitmap_default"] = "b:1;3\n";
        fakeIndex.indexes["bitmap2"] = "c:1;3\n";
        string queryStr = "default:a AND default:b#BITMAP AND bitmap2:c";
        string expectQueryStr = "BITMAPAND(a,TEST(b,c))";
        string unpackTerms = "a,b,c";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
}

TEST_F(QueryExecutorCreatorTest, testNumberTermBitmapTruncateName) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "1:1;3\n";
        fakeIndex.indexes["bitmap_default"] = "2:1;3\n";
        fakeIndex.indexes["bitmap2"] = "3:1;3\n";
        string queryStr = "default:1 AND default:2#BITMAP AND bitmap2:3";
        string expectQueryStr = "BITMAPAND(1,TEST(2,3))";
        string unpackTerms = "1,2,3";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
}

TEST_F(QueryExecutorCreatorTest, testBitmapNotBoth) {
    FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "A:1;3\n";
    fakeIndex.indexes["bitmap_default"] = "B:1;3\n";
    string queryStr = "default:A OR default:B#not_exist";
    string expectQueryStr = "OR(A,B)";
    string unpackTerms = "A,B";
    creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
}

TEST_F(QueryExecutorCreatorTest, testCreateSubTerm) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "A:0;1;2;4;7;9;13;14;\n";
    fakeIndex.indexes["sub_bitmap_index"] = "D:0;1;2;4;7;9;13;14;\n";
    fakeIndex.indexes["index_name"] = "B:1;3\n";
    fakeIndex.indexes["bitmap_index"] = "C:1;3\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,10,15");
    {
        string queryStr = "sub_index_name:A";
        string expectQueryStr = "sub_term:A";
        string unpackTerms = "A";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms, false, true);
    }

    {
        string queryStr = "sub_index_name:A AND index_name:B";
        string expectQueryStr = "AND(B,sub_term:A)";
        string unpackTerms = "A,B";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms, false, true);
    }

    {
        string queryStr = "sub_index_name:A AND bitmap_index:C";
        string expectQueryStr = "BITMAPAND(sub_term:A,TEST(C))";
        string unpackTerms = "A,C";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms, false, true);
    }
    {
        string queryStr = "sub_bitmap_index:D AND index_name:B";
        string expectQueryStr = "AND(B,sub_term:D)";
        string unpackTerms = "D,B";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms, false, true);
    }
}

TEST_F(QueryExecutorCreatorTest, testCreateSubPhrase) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index2"] = "A:0;1;2;4;7;9;13;14;\n";
    fakeIndex.indexes["sub_index1"] = "E:0;1;2;4;7;9;13;14;\n";
    fakeIndex.indexes["sub_bitmap_index"] = "D:0;1;2;4;7;9;13;14;\n";
    fakeIndex.indexes["index_name"] = "B:1;3\n";
    fakeIndex.indexes["bitmap_index"] = "C:1;3\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,10,15");
    {
        string queryStr = "sub_index_name:\"A E\"";
        string expectQueryStr = "PHRASE(A,E)";
        string unpackTerms = "A,E";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms, false, false);
    }

    {
        string queryStr = "sub_index_name:\"A B\"";
        string expectQueryStr = "PHRASE(A,B)";
        string unpackTerms = "A,B";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms, true, false);
    }
}

TEST_F(QueryExecutorCreatorTest, testMultiTermQuery) {
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "default:a & b";
        string expectQueryStr = "MultiTermAnd(a,b)";
        string unpackTerms = "a,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n";
        fakeIndex.indexes["bitmap_default"] = "b:1;3\n"
                                              "c:1;3\n";
        string queryStr = "default:a & b & c";
        string expectQueryStr = "MultiTermBITMAPAND(a,TEST(b,c))";
        string unpackTerms = "a,b,c";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "default:a | b";
        string expectQueryStr = "MultiTermOr(a,b)";
        string unpackTerms = "a,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;3\n"
                                       "b:2;3\n";
        string queryStr = "default:(a || b)^2";
        string expectQueryStr = "2WEAKAND(a,b)";
        string unpackTerms = "a,b";
        creatorTestInternal(fakeIndex, queryStr, expectQueryStr, unpackTerms);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;2\n";
        string queryStr = "default:b & a";
        string unpackTerms = "a, b";
        creatorTestInternal(fakeIndex, queryStr, "", "");
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "";
        string queryStr = "default:b | a";
        string unpackTerms = "a, b";
        creatorTestInternal(fakeIndex, queryStr, "", "");
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.indexes["default"] = "a:1;2\n";
        string queryStr = "default:(b || a)^2";
        string unpackTerms = "a, b";
        creatorTestInternal(fakeIndex, queryStr, "", "");
    }
}

template<typename T>
void QueryExecutorCreatorTest::singleColumnTableQueryTest(
        IndexPartitionReaderWrapperPtr indexPartReaderPtr,
        const string &indexName,
        const vector<T> &values,
        const vector<size_t> &offsets,
        const vector<docid_t> &docids,
        bool enableCache)
{
    auto term = new ColumnTermTyped<T>(indexName);
    term->setEnableCache(enableCache);
    term->getValues() = values;
    term->getOffsets() = offsets;
    TableQuery query("");
    query.getTermArray() = {term};
    expectSeekResult(query, indexPartReaderPtr, docids);
}

template<typename T>
void QueryExecutorCreatorTest::MultiColumnsTableQueryTest(
        IndexPartitionReaderWrapperPtr indexPartReaderPtr,
        const vector<string> &indexNames,
        const vector<vector<T>> &values,
        const vector<vector<size_t>> &offsets,
        const vector<docid_t> &docids)
{
    vector<ColumnTerm*> terms;
    for(size_t i = 0; i < indexNames.size(); ++i) {
        auto term = new ColumnTermTyped<T>(indexNames[i]);
        term->getValues() = values[i];
        term->getOffsets() = offsets[i];
        terms.push_back(term);
    }
    TableQuery query("");
    query.getTermArray() = terms;
    expectSeekResult(query, indexPartReaderPtr, docids);
}

template<>
void QueryExecutorCreatorTest::singleColumnTableQueryTest<string>(
        IndexPartitionReaderWrapperPtr indexPartReaderPtr,
        const string &indexName,
        const vector<string> &values,
        const vector<size_t> &offsets,
        const vector<docid_t> &docids,
        bool enableCache)
{
    vector<MultiChar> newValues(values.size());
    mem_pool::Pool pool;
    for (size_t i = 0; i < values.size(); ++i) {
        const auto &str = values[i];
        char *p = MultiValueCreator::createMultiValueBuffer<char>(
            str.data(), str.size(), &pool);
        MultiChar value(p);
        newValues[i] = value;
    }
    auto term = new ColumnTermTyped<MultiChar>(indexName);
    term->setEnableCache(enableCache);
    term->getValues() = newValues;
    term->getOffsets() = offsets;
    TableQuery query("");
    query.getTermArray() = {term};
    expectSeekResult(query, indexPartReaderPtr, docids);
}

template<>
void QueryExecutorCreatorTest::MultiColumnsTableQueryTest(
        IndexPartitionReaderWrapperPtr indexPartReaderPtr,
        const vector<string> &indexNames,
        const vector<vector<string>> &values,
        const vector<vector<size_t>> &offsets,
        const vector<docid_t> &docids)
{
    mem_pool::Pool pool;
    vector<ColumnTerm*> terms;
    for(size_t i = 0; i < indexNames.size(); ++i) {
        auto term = new ColumnTermTyped<MultiChar>(indexNames[i]);
        vector<MultiChar> newValues(values[i].size());
        for (size_t j = 0; j < values[i].size(); ++j) {
            const auto &str = values[i][j];
            char *p = MultiValueCreator::createMultiValueBuffer<char>(
                str.data(), str.size(), &pool);
            MultiChar value(p);
            newValues[j] = value;
        }
        term->getValues() = newValues;
        term->getOffsets() = offsets[i];
        terms.push_back(term);
    }
    TableQuery query("");
    query.getTermArray() = terms;
    expectSeekResult(query, indexPartReaderPtr, docids);
}

void QueryExecutorCreatorTest::expectSeekResult(
        const Query &query,
        IndexPartitionReaderWrapperPtr indexPartReaderPtr,
        const vector<docid_t> &docids)
{
    QueryExecutorCreator creator(nullptr, indexPartReaderPtr.get(), _pool);
    query.accept(&creator);
    QueryExecutor *queryExecutor = creator.stealQuery();
    vector<docid_t> results;
    docid_t id = 0;
    while (END_DOCID != (id = queryExecutor->legacySeek(id))) {
        results.push_back(id);
        ++id;
    }
    EXPECT_EQ(results, docids);
    queryExecutor->reset();
    POOL_DELETE_CLASS(queryExecutor);
}

void QueryExecutorCreatorTest::checkTableQuery(
        IndexPartitionReaderWrapperPtr readerWrapper,
        bool enableCache)
{
    singleColumnTableQueryTest<int64_t>(
        readerWrapper, "group", {101, 103, 101}, {}, {0, 1, 2, 6, 7, 8, 9}, enableCache);
    singleColumnTableQueryTest<int32_t>(
        readerWrapper, "group", {101, 103, 103}, {}, {0, 1, 2, 6, 7, 8, 9}, enableCache);
    singleColumnTableQueryTest<int16_t>(
        readerWrapper, "group", {101, 103, 101}, {}, {0, 1, 2, 6, 7, 8, 9}, enableCache);
    singleColumnTableQueryTest<int8_t>(
        readerWrapper, "group", {101, 103, 101}, {}, {0, 1, 2, 6, 7, 8, 9}, enableCache);
    singleColumnTableQueryTest<uint64_t>(
        readerWrapper, "group", {101, 103, 103}, {}, {0, 1, 2, 6, 7, 8, 9}, enableCache);
    singleColumnTableQueryTest<uint32_t>(
        readerWrapper, "group", {101, 103, 103}, {}, {0, 1, 2, 6, 7, 8, 9}, enableCache);
    singleColumnTableQueryTest<uint16_t>(
        readerWrapper, "group", {101, 103, 103}, {}, {0, 1, 2, 6, 7, 8, 9}, enableCache);
    singleColumnTableQueryTest<uint8_t>(
        readerWrapper, "group", {101, 103, 101}, {}, {0, 1, 2, 6, 7, 8, 9}, enableCache);
    singleColumnTableQueryTest<string>(
        readerWrapper, "group", {"101", "103", "103"}, {}, {0, 1, 2, 6, 7, 8, 9}, enableCache);
    singleColumnTableQueryTest<int64_t>(
        readerWrapper, "id", {2, 5, 5, 2}, {}, {1, 4}, enableCache);
    singleColumnTableQueryTest<string>(
        readerWrapper, "id", {"2", "5", "2"}, {}, {1, 4}, enableCache);
    singleColumnTableQueryTest<int64_t>(
        readerWrapper, "tag", {1001, 1003}, {}, {0, 1, 4, 5}, enableCache);
    singleColumnTableQueryTest<string>(
        readerWrapper, "tag", {"1001", "1003", "1001", "1003"}, {}, {0, 1, 4, 5}, enableCache);
    singleColumnTableQueryTest<int64_t>(
        readerWrapper, "id", {1, 2, 3, 5, 9}, {0, 3, 4, 5}, {0, 1, 2, 4, 8}, enableCache);
    singleColumnTableQueryTest<string>(
        readerWrapper, "id", {"1", "2", "3", "9", "5"}, {0, 3, 3, 4, 5}, {0, 1, 2, 4, 8}, enableCache);
    //bad cases
    singleColumnTableQueryTest<int64_t>(
        readerWrapper, "id", {1, 2, 3, 5, 900}, {0, 3, 4, 5}, {0, 1, 2, 4}, enableCache);
    singleColumnTableQueryTest<int64_t>(
        readerWrapper, "unknownIndex", {1, 2, 3, 5, 900}, {0, 3, 4, 5}, {}, enableCache);
    //MultiColumn
    MultiColumnsTableQueryTest<int64_t>(
        readerWrapper, {"id","group"}, {{1, 2}, {101, 103}}, {{},{}}, {0});
}

TEST_F(QueryExecutorCreatorTest, testTableQueryExecutor) {
    string field = "id:int64:false;group:int64:false;tag:string:false";
    string index = "id:primarykey64:id;group:number:group;tag:string:tag";
    string attribute = "id";
    string summary = "";

    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema("table", field, index, attribute, "");
    string docString = "cmd=add,id=1,group=101,tag=1001;"
                       "cmd=add,id=2,group=101,tag=1001;"
                       "cmd=add,id=3,group=101,tag=1002;"
                       "cmd=add,id=4,group=102,tag=1002;"
                       "cmd=add,id=5,group=102,tag=1003;"
                       "cmd=add,id=6,group=102,tag=1003;"
                       "cmd=add,id=7,group=103,tag=1004;"
                       "cmd=add,id=8,group=103,tag=1004;"
                       "cmd=add,id=9,group=103,tag=1005;"
                       "cmd=add,id=10,group=103,tag=1005;";

    IndexPartitionPtr indexPartition =
        IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                schema, GET_TEMPLATE_DATA_PATH(), docString);
    vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(indexPartition);
    ReaderWrapperContainer container;
    IndexPartitionReaderWrapperCreator::CreateIndexPartitionReaderWrapper(
            indexPartitions, container);
    checkTableQuery(container.readerWrapper, true);
    checkTableQuery(container.readerWrapper, false);
}

void QueryExecutorCreatorTest::creatorTestInternal(
        const IndexPartitionReaderWrapperPtr& indexPartReaderPtr,
        const string &queryStr, const string &expectQueryStr, const string &unpackTerms,
        bool movedToEnd, bool hasSubDocExecutor)
{
    indexPartReaderPtr->setTopK(1000);
    MatchDataManager matchDataManager;
    matchDataManager.setQueryCount(1);
    QueryExecutorCreator creator(&matchDataManager, indexPartReaderPtr.get(), _pool);

    unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));
    query->accept(&creator);
    QueryExecutor *queryExecutor = creator.stealQuery();

    if (expectQueryStr.empty()) {
        ASSERT_TRUE(queryExecutor);
        ASSERT_TRUE(queryExecutor->isEmpty());
        POOL_DELETE_CLASS(queryExecutor);
        return;
    }
    ASSERT_TRUE(queryExecutor);
    ASSERT_EQ(expectQueryStr, queryExecutor->toString());
    checkUnpackTerms(matchDataManager, unpackTerms);
    if (movedToEnd) {
        ASSERT_EQ(END_DOCID, queryExecutor->getDocId());
    }
    HA3_LOG(INFO, "query is [%s]", queryStr.c_str());
    if (hasSubDocExecutor) {
        assert(queryExecutor->hasSubDocExecutor());
    } else {
        assert(!queryExecutor->hasSubDocExecutor());
    }
    POOL_DELETE_CLASS(queryExecutor);
}

void QueryExecutorCreatorTest::creatorTestInternal(const FakeIndex &fakeIndex,
        const string &queryStr, const string &expectQueryStr, const string &unpackTerms,
        bool movedToEnd, bool hasSubDocExecutor)
{
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    creatorTestInternal(indexPartReaderPtr, queryStr,
                        expectQueryStr, unpackTerms, movedToEnd,
                        hasSubDocExecutor);
}

void QueryExecutorCreatorTest::checkUnpackTerms(
        MatchDataManager &matchDataManager, const string &unpackTerms)
{
    StringTokenizer st(unpackTerms, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    HA3_LOG(INFO, "unpackTerms is [%s]", unpackTerms.c_str());
    assert((size_t)st.getNumTokens() == matchDataManager.getQueryNum());
    MetaInfo metaInfo;
    matchDataManager.getQueryTermMetaInfo(&metaInfo);
    ASSERT_EQ((size_t)metaInfo.size(), (size_t)st.getNumTokens());
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        ASSERT_EQ(string(metaInfo[i].getTermText().c_str()), st[i]);
    }
}

void QueryExecutorCreatorTest::createIndexPartitionReaderWrapper(
        const string &indexStr)
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = indexStr;
    _indexPartitionReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexPartitionReaderWrapper->setTopK(1000);
}

IndexReaderPtr QueryExecutorCreatorTest::getFakeTextIndexReader() {
    assert(_indexPartitionReaderWrapper);
    IndexReaderPtr indexReader =
        _indexPartitionReaderWrapper->getIndexReader("indexname");
    FakeIndexReader *fakeIndexReader =
        dynamic_cast<FakeIndexReader *>(indexReader.get());
    assert(fakeIndexReader != NULL);
    indexReader = fakeIndexReader->getIndexReader("indexname");
    return indexReader;
}

END_HA3_NAMESPACE(search);
