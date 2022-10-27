#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sorter/DefaultSorter.h>
#include <ha3/common/SortClause.h>
#include <ha3/rank/TestScorer.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <suez/turing/common/FileUtil.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <indexlib/partition/index_application.h>

using namespace std;
using namespace autil::mem_pool;
using namespace suez::turing;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(rank);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(partition);
BEGIN_HA3_NAMESPACE(sorter);

class DefaultSorterTest : public TESTBASE {
public:
    DefaultSorterTest();
    ~DefaultSorterTest();
public:
    void setUp();
    void tearDown();
protected:
    SorterProviderPtr prepareSorterProvider(common::Request *request,
            SorterLocation sl = SL_SEARCHER, uint32_t resultSourceNum = 1);
    void innerTestValidateSortInfo(const std::string &requestStr, bool valid);
    void innerTestNeedScoreInSort(const std::string &requestStr, bool need);
    void innerTestNeedSort(const std::string &requestStr, SorterLocation sl,
                           uint32_t resultSourceNum,  bool need);
    void prepareIndex();
    void removeIndex();
    void release(SorterResource &sorterResource);

protected:
    autil::mem_pool::Pool *_pool;
    uint32_t _requiredTopk;
    common::MultiErrorResultPtr _errorResult;
    IE_NAMESPACE(partition)::IndexApplication::IndexPartitionMap _indexPartitionMap;
    IE_NAMESPACE(partition)::IndexApplicationPtr _indexApp;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    suez::turing::TableInfoPtr _tableInfoPtr;
    string _tableName;
    string _indexRootPath;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sorter, DefaultSorterTest);

DefaultSorterTest::DefaultSorterTest() {
}

DefaultSorterTest::~DefaultSorterTest() {
}

void DefaultSorterTest::prepareIndex() {

    auto schema = IndexlibPartitionCreator::CreateSchema(_tableName,
            "pk:string;uid:int32;uid1:int32;uid2:int32;uid3:int32;", // fields
            "pk:primarykey64:pk", //indexs
            "uid;uid1;uid2;uid3", //attributes
            "", // summarys
            ""); // truncateProfile
    string docStrs = "cmd=add,pk=pk0,uid=1,uid1=11,uid2=21,uid3=31;"
                     "cmd=add,pk=pk1,uid=2,uid1=12,uid2=22,uid3=32;"
                     "cmd=add,pk=pk2,uid=3,uid1=13,uid2=23,uid3=33;"
                     "cmd=add,pk=pk3,uid=4,uid1=14,uid2=24,uid3=34;"
                     "cmd=add,pk=pk4,uid=5,uid1=15,uid2=25,uid3=35;"
                     "cmd=add,pk=pk5,uid=6,uid1=16,uid2=26,uid3=36;"
                     "cmd=add,pk=pk6,uid=7,uid1=17,uid2=27,uid3=37;"
                     "cmd=add,pk=pk7,uid=8,uid1=18,uid2=28,uid3=38;"
                     "cmd=add,pk=pk8,uid=9,uid1=19,uid2=29,uid3=39;"
                     "cmd=add,pk=pk9,uid=10,uid1=20,uid2=30,uid3=40;";

    auto indexPartition = IndexlibPartitionCreator::CreateIndexPartition(
            schema, _indexRootPath + _tableName, docStrs);
    string schemaName = indexPartition->GetSchema()->GetSchemaName();
    _indexPartitionMap.insert(make_pair(schemaName, indexPartition));

    _indexApp.reset(new IndexApplication);
    _indexApp->Init(_indexPartitionMap, JoinRelationMap());
    _snapshotPtr = _indexApp->CreateSnapshot();
    _tableInfoPtr = TableInfoConfigurator::createFromIndexApp(_tableName, _indexApp);
}

void DefaultSorterTest::removeIndex() {
    FileUtil::removeLocalDir(_indexRootPath + _tableName, true);
}

SorterProviderPtr DefaultSorterTest::prepareSorterProvider(Request *request,
        SorterLocation sl, uint32_t resultSourceNum)
{
    common::Ha3MatchDocAllocatorPtr allocator(new common::Ha3MatchDocAllocator(_pool));
    VirtualAttributes virtualAttributes;
    AttributeExpressionCreator *attrExprCreator =
        new AttributeExpressionCreator(_pool,
                allocator.get(), _tableName, _snapshotPtr.get(), _tableInfoPtr,
                virtualAttributes, nullptr, CavaPluginManagerPtr(), nullptr);
    SortExpressionCreator *sortExprCreator = new SortExpressionCreator(
            attrExprCreator, NULL, allocator.get(), _errorResult, _pool);
    matchdoc::Reference<score_t> *scoreRef = allocator->declare<score_t>("scoreref");
    RankAttributeExpression *rankExpression = new RankAttributeExpression(
            new TestScorer(), scoreRef);
    rankExpression->allocate(allocator.get());

    SorterResource sorterResource;
    sorterResource.location = sl;
    sorterResource.scoreExpression = rankExpression;
    sorterResource.pool = _pool;
    sorterResource.cavaAllocator = nullptr;
    sorterResource.request = request;
    sorterResource.attrExprCreator = attrExprCreator;
    sorterResource.dataProvider = new DataProvider();
    sorterResource.matchDocAllocator = allocator;
    sorterResource.fieldInfos = NULL;
    sorterResource.comparatorCreator = new ComparatorCreator(_pool);
    sorterResource.sortExprCreator = sortExprCreator;
    sorterResource.resultSourceNum = resultSourceNum;
    _requiredTopk = 10;
    sorterResource.requiredTopk = &_requiredTopk;

    SorterProviderPtr sorterProviderPtr(new SorterProvider(sorterResource));
    return sorterProviderPtr;
}

void DefaultSorterTest::release(SorterResource &sorterResource) {
    DELETE_AND_SET_NULL(sorterResource.scoreExpression);
    DELETE_AND_SET_NULL(sorterResource.attrExprCreator);
    DELETE_AND_SET_NULL(sorterResource.dataProvider);
    DELETE_AND_SET_NULL(sorterResource.comparatorCreator);
    DELETE_AND_SET_NULL(sorterResource.sortExprCreator);
}

void DefaultSorterTest::setUp() {
    _pool = new Pool();
    _tableName = "mainTable";
    _indexRootPath =  GET_TEMP_DATA_PATH() + "DefaultSorter";
    prepareIndex();
}

void DefaultSorterTest::tearDown() {
    removeIndex();
    delete _pool;
}

TEST_F(DefaultSorterTest, testAddSortExprReferenceAndAddSortExprMeta) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string query = "config=cluster:cluster1,rank_size:10&&query=phrase:with"
                       "&&sort=RANK&&rank_sort=sort:uid1,percent:50;sort:uid2,percent:50;";
        RequestPtr requestPtr = RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        SorterProviderPtr sorterProviderPtr = prepareSorterProvider(requestPtr.get());
        AttributeExpression *rankExpression = sorterProviderPtr->getSorterResource().scoreExpression;
        DefaultSorter sorter;
        ASSERT_TRUE(sorter.beginSort(sorterProviderPtr.get()));
        ASSERT_EQ(SL_CACHE, rankExpression->getReferenceBase()->getSerializeLevel());

        const vector<SortExprMeta>& sortExprMetas =
            sorterProviderPtr->getSortExprMeta();
         ASSERT_EQ(size_t(1), sortExprMetas.size());
         ASSERT_EQ(string("RANK"), sortExprMetas[0].sortExprName);
         ASSERT_EQ((const matchdoc::ReferenceBase*)rankExpression->getReferenceBase(),
                              sortExprMetas[0].sortRef);
         ASSERT_TRUE(!sortExprMetas[0].sortFlag);
         release(*sorterProviderPtr->getInnerSorterResource());
    }

    {
        string query = "config=cluster:cluster1,rank_size:10&&query=phrase:with";
        RequestPtr requestPtr = RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        SorterProviderPtr sorterProviderPtr = prepareSorterProvider(requestPtr.get());
        DefaultSorter sorter;
        AttributeExpression *rankExpression = sorterProviderPtr->getSorterResource().scoreExpression;
        ASSERT_TRUE(sorter.beginSort(sorterProviderPtr.get()));
        ASSERT_EQ(SL_CACHE, rankExpression->getReferenceBase()->getSerializeLevel());
        const vector<SortExprMeta>& sortExprMetas =
            sorterProviderPtr->getSortExprMeta();
         ASSERT_EQ(size_t(1), sortExprMetas.size());
         ASSERT_EQ(string("RANK"), sortExprMetas[0].sortExprName);
         ASSERT_EQ((const matchdoc::ReferenceBase*)rankExpression->getReferenceBase(),
                              sortExprMetas[0].sortRef);
         ASSERT_TRUE(!sortExprMetas[0].sortFlag);
         release(*sorterProviderPtr->getInnerSorterResource());
    }

    {
        string query = "config=cluster:cluster1,rank_size:10&&query=phrase:with"
                       "&&sort=RANK&&rank_sort=sort:uid1,percent:50;sort:uid2,percent:50;"
                       "&&final_sort=sort_name:DEFAULT;sort_param:sort_key#+uid1|-uid2";
        RequestPtr requestPtr = RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        SorterProviderPtr sorterProviderPtr = prepareSorterProvider(requestPtr.get());
        Ha3MatchDocAllocatorPtr allocator = sorterProviderPtr->getSorterResource().matchDocAllocator;
        DefaultSorter sorter;
        ASSERT_TRUE(sorter.beginSort(sorterProviderPtr.get()));
        matchdoc::ReferenceBase *v1 = allocator->findReferenceWithoutType("uid1");
        matchdoc::ReferenceBase *v2 = allocator->findReferenceWithoutType("uid2");
        ASSERT_EQ(SL_CACHE, v1->getSerializeLevel());
        ASSERT_EQ(SL_CACHE, v2->getSerializeLevel());

        const vector<SortExprMeta>& sortExprMetas =
            sorterProviderPtr->getSortExprMeta();
         ASSERT_EQ(size_t(2), sortExprMetas.size());
         ASSERT_EQ(string("uid1"), sortExprMetas[0].sortExprName);
         ASSERT_EQ((const matchdoc::ReferenceBase*)v1, sortExprMetas[0].sortRef);
         ASSERT_TRUE(sortExprMetas[0].sortFlag);
         ASSERT_EQ(string("uid2"), sortExprMetas[1].sortExprName);
         ASSERT_EQ((const matchdoc::ReferenceBase*)v2, sortExprMetas[1].sortRef);
         ASSERT_TRUE(!sortExprMetas[1].sortFlag);
         release(*sorterProviderPtr->getInnerSorterResource());
    }
}

TEST_F(DefaultSorterTest, testValidateSortInfo) {
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:sort_key#id1|id2";
        innerTestValidateSortInfo(query, true);
    }
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:key#value";
        innerTestValidateSortInfo(query, true);
    }
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:key#value&&sort=id";
        innerTestValidateSortInfo(query, true);
    }
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:key#value&&rank_sort=sort:id,percent:100";
        innerTestValidateSortInfo(query, true);
    }
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:key#value&&"
                       "rank_sort=sort:id1#id2,percent:50;sort:id3#id4,percent:50;";
        innerTestValidateSortInfo(query, false);
    }
    {
        string query = "query=mp3";
        innerTestValidateSortInfo(query, true);
    }
}

TEST_F(DefaultSorterTest, testNeedScoreInSort) {
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:sort_key#id1|id2";
        innerTestNeedScoreInSort(query, false);
    }
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:sort_key#id1|id2|RANK";
        innerTestNeedScoreInSort(query, true);
    }
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:key#value";
        innerTestNeedScoreInSort(query, true);
    }
    {
        string query = "query=mp3";
        innerTestNeedScoreInSort(query, true);
    }
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:key#value&&sort=id";
        innerTestNeedScoreInSort(query, false);
    }
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:key#value&&sort=RANK";
        innerTestNeedScoreInSort(query, true);
    }
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:key#value&&rank_sort=sort:id1#id2,percent:100";
        innerTestNeedScoreInSort(query, false);
    }
    {
        string query = "final_sort=sort_name:DEFAULT;sort_param:key#value&&rank_sort=sort:id1#RANK,percent:100";
        innerTestNeedScoreInSort(query, true);
    }
}

TEST_F(DefaultSorterTest, testNeedSort) {
    {
        string query = "query=abc1&&searcher_cache=use:yes,key:123,refresh_attributes:attr1";
        innerTestNeedSort(query, SL_SEARCHER, 1, true);
    }
    {
        string query = "query=abc2&&searcher_cache=use:yes,key:123,refresh_attributes:attr1";
        innerTestNeedSort(query, SL_QRS, 2, true);
    }
    {
        string query = "query=abc3&&searcher_cache=use:yes,key:123,refresh_attributes:attr1";
        innerTestNeedSort(query, SL_QRS, 1, false);
    }
    {
        string query = "query=abc4&&searcher_cache=use:yes,key:123,refresh_attributes:attr1";
        innerTestNeedSort(query, SL_SEARCHCACHEMERGER, 2, true);
    }
    {
        string query = "query=abc5&&searcher_cache=use:yes,key:123,refresh_attributes:attr1";
        innerTestNeedSort(query, SL_SEARCHCACHEMERGER, 1, true);
    }
    {
        string query = "query=abc6&&searcher_cache=use:yes,key:123";
        innerTestNeedSort(query, SL_SEARCHCACHEMERGER, 1, false);
    }
}

TEST_F(DefaultSorterTest, testBeginSortWithComboAndRankExpr) {
    string query = "config=cluster:cluster1,rank_size:10&&query=phrase:with"
                   "&&sort=uid;RANK;uid1";
    RequestPtr requestPtr = RequestCreator::prepareRequest(query);
    ASSERT_TRUE(requestPtr);
    SortClause *sortClause = requestPtr->getSortClause();
    auto sortDescs = sortClause->getSortDescriptions();
    ASSERT_EQ(size_t(3), sortDescs.size());
    sortDescs[0]->getRootSyntaxExpr()->setExprResultType(vt_int32);
    sortDescs[2]->getRootSyntaxExpr()->setExprResultType(vt_int32);

    SorterProviderPtr sorterProviderPtr = prepareSorterProvider(requestPtr.get());
    DefaultSorter sorter;
    ASSERT_TRUE(sorter.beginSort(sorterProviderPtr.get()));
    const ExpressionVector &needEvaluateExprs =
        sorterProviderPtr->getNeedEvaluateAttrs();
    ASSERT_EQ(size_t(2), needEvaluateExprs.size());

    SorterResource *sorterResource = sorterProviderPtr->getInnerSorterResource();
    PoolVector<matchdoc::MatchDoc> matchDocs(_pool);
    RankAttributeExpression *rankExpression = dynamic_cast<RankAttributeExpression*>(sorterResource->scoreExpression);
    matchdoc::Reference<score_t> *scoreRef = rankExpression->getReference();
    for (size_t i = 0; i < 8; i++) {
        matchdoc::MatchDoc matchDoc = sorterResource->matchDocAllocator->allocate(i);
        score_t score = (score_t)(8 - i);
        scoreRef->set(matchDoc, score);
        matchDocs.push_back(matchDoc);
    }
    SortParam sortParam(_pool);
    sortParam.requiredTopK = 10;
    sortParam.matchDocs.swap(matchDocs);

    ExpressionEvaluator<ExpressionVector> evaluator(needEvaluateExprs,
            sorterResource->matchDocAllocator->getSubDocAccessor());
    evaluator.batchEvaluateExpressions(&(sortParam.matchDocs[0]),
            sortParam.matchDocs.size());

    matchdoc::Reference<int32_t> *uidRef = dynamic_cast<matchdoc::Reference<int32_t>*>(
            needEvaluateExprs[0]->getReferenceBase());
    matchdoc::Reference<int32_t> *uid1Ref = dynamic_cast<matchdoc::Reference<int32_t>*>(
            needEvaluateExprs[1]->getReferenceBase());
    for (size_t i = 0; i < sortParam.matchDocs.size(); i++) {
        matchdoc::MatchDoc matchDoc = sortParam.matchDocs[i];
        ASSERT_EQ((int32_t)(i+1),
                             uidRef->getReference(matchDoc));
        ASSERT_EQ((int32_t)(i+11),
                             uid1Ref->getReference(matchDoc));
        sorterResource->matchDocAllocator->deallocate(matchDoc);
    }
    release(*sorterResource);
}

void DefaultSorterTest::innerTestValidateSortInfo(const string &requestStr, bool valid) {
    RequestPtr requestPtr = RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    DefaultSorter sorter;
    ASSERT_EQ(valid, sorter.validateSortInfo(requestPtr.get()));
}

void DefaultSorterTest::innerTestNeedScoreInSort(const string &requestStr, bool need) {
    RequestPtr requestPtr = RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    DefaultSorter sorter;
    ASSERT_EQ(need, sorter.needScoreInSort(requestPtr.get()));
}

void DefaultSorterTest::innerTestNeedSort(const string &requestStr, SorterLocation sl,
        uint32_t resultSourceNum,  bool need) {

    RequestPtr requestPtr = RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    SorterProviderPtr sorterProviderPtr = prepareSorterProvider(requestPtr.get(), sl, resultSourceNum);
    DefaultSorter sorter;
    ASSERT_EQ(need, sorter.needSort(sorterProviderPtr.get())) << requestStr.c_str();
    release(*sorterProviderPtr->getInnerSorterResource());
}


END_HA3_NAMESPACE(sorter);
