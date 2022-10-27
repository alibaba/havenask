#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/AuxiliaryChainOptimizer.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexReader.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class AuxiliaryChainOptimizerTest : public TESTBASE {
public:
    AuxiliaryChainOptimizerTest();
    ~AuxiliaryChainOptimizerTest();
public:
    void setUp();
    void tearDown();
protected:
    void internalOptimizerTest(const std::string &termDFMapStr, const std::string &queryStr,
                               const std::string &expectQuerysStr,
                               QueryInsertType queryInsertType = QI_OVERWRITE,
                               SelectAuxChainType selectAuxChainType = SAC_DF_SMALLER);
    IE_NAMESPACE(partition)::IndexPartitionReaderPtr createIndexPartitionReader(
            const TermDFMap &termDFMap);

protected:
    std::vector<common::Query*> _querys;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, AuxiliaryChainOptimizerTest);


AuxiliaryChainOptimizerTest::AuxiliaryChainOptimizerTest() { 
}

AuxiliaryChainOptimizerTest::~AuxiliaryChainOptimizerTest() { 
}

void AuxiliaryChainOptimizerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AuxiliaryChainOptimizerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    for (size_t i = 0; i < _querys.size(); ++i) {
        delete _querys[i];
    }
    _querys.clear();
}

TEST_F(AuxiliaryChainOptimizerTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    internalOptimizerTest("a:10,b:5", "a AND b", "a AND b#auxChain");
    internalOptimizerTest("a:10,b:5", "a AND b", "a#auxChain AND b#auxChain;a AND b", QI_BEFORE, SAC_ALL);
    internalOptimizerTest("a:10,b:5", "a AND b", "a AND b;a#auxChain AND b#auxChain", QI_AFTER, SAC_ALL);
    internalOptimizerTest("a:10,b:15", "a OR b", "a#auxChain OR b#auxChain", QI_OVERWRITE);
    internalOptimizerTest("a:10,b:15", "a OR b", "a#auxChain OR b#auxChain;a OR b", QI_BEFORE);
    internalOptimizerTest("a:10,b:15", "a OR b", "a OR b;a#auxChain OR b#auxChain", QI_AFTER);
    internalOptimizerTest("a:10,b:15,c:20", "a AND (b ANDNOT c)", "a#auxChain AND (b ANDNOT c)");
    internalOptimizerTest("ia:a:10,ib:a:15", "ia:a AND ib:a", "ia:a#auxChain AND ib:a");
    internalOptimizerTest("ia:a:20,ib:a:15", "ia:a AND ib:a", "ia:a AND ib:a#auxChain");
    internalOptimizerTest("a:10,b:15", "a AND (b ANDNOT c)", "a#auxChain AND (b ANDNOT c)");
    internalOptimizerTest("", "a AND (b ANDNOT c)", "a#auxChain AND (b ANDNOT c)");
    internalOptimizerTest("a:10,b:15,c:20,e:25", "(a RANK d) AND ((b ANDNOT c) OR e)", "(a#auxChain RANK d) AND ((b ANDNOT c) OR e)");
    internalOptimizerTest("a:10,b:15,c:20,e:25", "(a RANK d) AND ((b#auxChain ANDNOT c) OR e)", "(a#auxChain RANK d) AND ((b#auxChain ANDNOT c) OR e)");
    internalOptimizerTest("a:10,b:15,c:20,e:25", "(a#unexit RANK d) AND ((b ANDNOT c) OR e)", "(a#auxChain RANK d) AND ((b ANDNOT c) OR e)");

    internalOptimizerTest("ia:a:15,ib:a:15,ia:c:13,c:20,d:10,e:12,f:13,", "ia:a AND ib:a AND ia:c AND d AND c", "ia:a AND ib:a AND ia:c AND d#auxChain AND c");
}

TEST_F(AuxiliaryChainOptimizerTest, testDisableTruncate) {
    AuxiliaryChainOptimizer optimizer;
    string option = "aux_name#BITMAP|select#ALL";
    OptimizeInitParam param(option, NULL);
    ASSERT_TRUE(optimizer.init(&param));
    optimizer.disableTruncate();
    ASSERT_TRUE(optimizer.isActive());

    string option1 = "aux_name#phrase_asc_ends|select#ALL";
    OptimizeInitParam param1(option1, NULL);
    ASSERT_TRUE(optimizer.init(&param1));
    optimizer.disableTruncate();
    ASSERT_TRUE(!optimizer.isActive());
}

void AuxiliaryChainOptimizerTest::internalOptimizerTest(const string &termDFMapStr,
        const string &queryStr, const string &expectQuerysStr,
        QueryInsertType queryInsertType, SelectAuxChainType selectAuxChainType)
{
    tearDown();
    TermDFMap termDFMap = SearcherTestHelper::createTermDFMap(termDFMapStr);
    IndexPartitionReaderPtr reader = createIndexPartitionReader(termDFMap);

    Request request;
    Query *query = SearcherTestHelper::createQuery(queryStr);
    QueryClause *queryClause = new QueryClause();
    queryClause->setRootQuery(query);
    request.setQueryClause(queryClause);

    AuxiliaryChainOptimizer optimizer;
    optimizer._queryInsertType = queryInsertType;
    optimizer._selectAuxChainType = selectAuxChainType;
    optimizer._auxChainName = "auxChain";

    vector<IndexPartitionReaderPtr> indexPartReaderPtrs;
    indexPartReaderPtrs.push_back(reader);
    map<string, uint32_t> attrName2IdMap;
    IndexPartitionReaderWrapper wrapper(NULL, &attrName2IdMap, indexPartReaderPtrs);
    wrapper.setTopK(1);
    OptimizeParam param;
    param.request = &request;
    param.readerWrapper = &wrapper;

    ASSERT_TRUE(optimizer.optimize(&param));

    queryClause = request.getQueryClause();
    _querys = SearcherTestHelper::createQuerys(expectQuerysStr);

    ASSERT_EQ((uint32_t)_querys.size(), queryClause->getQueryCount());
    for (size_t i = 0; i < _querys.size(); ++i) {
        ASSERT_EQ(*(_querys[i]), *(queryClause->getRootQuery(i))) << expectQuerysStr;
    }
}

IndexPartitionReaderPtr AuxiliaryChainOptimizerTest::createIndexPartitionReader(
        const TermDFMap &termDFMap)
{
    FakeIndexPartitionReader *fakePartitionReader = new FakeIndexPartitionReader;
    map<string, stringstream*> ssMap;
    for (TermDFMap::const_iterator it = termDFMap.begin();
         it != termDFMap.end(); ++it)
    {
        const string &term = it->first.getWord();
        if (ssMap.find(it->first.getIndexName()) == ssMap.end()) {
            ssMap[it->first.getIndexName()] = new stringstream;
        }
        stringstream *ss = ssMap[it->first.getIndexName()];
        df_t df = it->second;
        *ss << term << ":";
        for (int i = 0; i < df; ++i) {
            *ss << i << "[1];";
        }
        *ss << "\n";
    }
    FakeIndexReader *indexReader = new FakeIndexReader;
    for (map<string, stringstream*>::const_iterator it = ssMap.begin();
         it != ssMap.end(); ++it)
    {
        indexReader->addIndexReader(it->first, new FakeTextIndexReader(it->second->str()));
        delete it->second;
    }
    fakePartitionReader->SetIndexReader(IndexReaderPtr(indexReader));
    return IndexPartitionReaderPtr(fakePartitionReader);
}

END_HA3_NAMESPACE(search);

