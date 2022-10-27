#include<unittest/unittest.h>
#include <autil/StringUtil.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sorter/SorterProvider.h>
#include <ha3/rank/ReferenceComparator.h>
#include <matchdoc/MatchDocAllocator.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3/rank/test/FakeExpression.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/framework/RankAttributeExpression.h>
#include <ha3/search/ConstAttributeExpression.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <indexlib/partition/index_application.h>
#include <ha3/rank/TestScorer.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;

IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(testlib);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sorter);

class SorterProviderTest : public TESTBASE {
public:
    SorterProviderTest();
    ~SorterProviderTest();
public:
    void setUp();
    void tearDown();
protected:
    void innerTestDeclareReference(const SorterResource &resource);
    void innerTestGlobalDeclareReference(const SorterResource &resource);
protected:
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sorter, SorterProviderTest);


SorterProviderTest::SorterProviderTest() {
    _pool = new Pool();
}

SorterProviderTest::~SorterProviderTest() {
    delete _pool;
}

void SorterProviderTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void SorterProviderTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SorterProviderTest, testCreateSingleComparator) {
    SorterResource resource;
    resource.resultSourceNum = 3;
    resource.pool = _pool;

    matchdoc::MatchDocAllocator slabAllocator(_pool);
    matchdoc::ReferenceBase *ref = slabAllocator.declare<int32_t>("test");

    SorterProvider provider(resource);
    ComparatorItem item(ref, true);

    ComparatorItemVec cmpItems;
    cmpItems.push_back(item);
    ComboComparator *cmp = provider.createComparator(cmpItems);

    const ComboComparator::ComparatorVector &cmps = cmp->getComparators();
    ASSERT_EQ((size_t)1, cmps.size());
    const ReferenceComparator<int32_t> *refCmp =
        dynamic_cast<const ReferenceComparator<int32_t>* >(cmps[0]);
    ASSERT_TRUE(refCmp != NULL);
    ASSERT_EQ(true, refCmp->getReverseFlag());
    POOL_DELETE_CLASS(cmp);

    ASSERT_EQ((uint32_t)3, provider.getResultSourceNum());
}

TEST_F(SorterProviderTest, testCreateComparatorWithString) {
    SorterResource resource;
    resource.resultSourceNum = 3;
    resource.pool = _pool;

    matchdoc::MatchDocAllocator slabAllocator(_pool);
    matchdoc::ReferenceBase *ref1 = slabAllocator.declare<MultiChar>("test");

    SorterProvider provider(resource);
    ComparatorItem item1(ref1, true);

    ComparatorItemVec cmpItems;
    cmpItems.push_back(item1);
    ComboComparator *cmp = provider.createComparator(cmpItems);
    ASSERT_TRUE(cmp != NULL);

    const ComboComparator::ComparatorVector &cmps = cmp->getComparators();
    ASSERT_EQ((size_t)1, cmps.size());
    const ReferenceComparator<MultiChar> *refCmp =
        dynamic_cast<const ReferenceComparator<MultiChar>* >(cmps[0]);
    ASSERT_TRUE(refCmp != NULL);
    ASSERT_EQ(true, refCmp->getReverseFlag());
    POOL_DELETE_CLASS(cmp);
}

TEST_F(SorterProviderTest, testCreateComparatorFail) {
    SorterResource resource;
    resource.resultSourceNum = 3;
    resource.pool = _pool;

    matchdoc::MatchDocAllocator slabAllocator(_pool);
    matchdoc::ReferenceBase *ref1 = slabAllocator.declare<MultiChar>("test1");
    matchdoc::ReferenceBase *ref2 = slabAllocator.declare<MultiInt8>("test2");

    SorterProvider provider(resource);
    ComparatorItem item1(ref1, true);
    ComparatorItem item2(ref2, false);

    ComparatorItemVec cmpItems;
    cmpItems.push_back(item1);
    cmpItems.push_back(item2);
    ComboComparator *cmp = provider.createComparator(cmpItems);
    ASSERT_TRUE(cmp == NULL);
}

TEST_F(SorterProviderTest, testCreateComboComparator) {
    SorterResource resource;
    resource.pool = _pool;

    ComparatorItemVec cmpItems;
    matchdoc::MatchDocAllocator slabAllocator(_pool);
    matchdoc::ReferenceBase *ref = slabAllocator.declare<int32_t>("test");
    ComparatorItem item(ref, true);
    cmpItems.push_back(item);

    ref = slabAllocator.declare<float>("test2");
    ComparatorItem item2(ref, false);
    cmpItems.push_back(item2);

    SorterProvider provider(resource);
    ComboComparator *cmp = provider.createComparator(cmpItems);

    const ComboComparator::ComparatorVector &cmps = cmp->getComparators();
    ASSERT_EQ((size_t)2, cmps.size());
    const ReferenceComparator<int32_t> *refCmp =
        dynamic_cast<const ReferenceComparator<int32_t>* >(cmps[0]);
    ASSERT_TRUE(refCmp != NULL);
    ASSERT_EQ(true, refCmp->getReverseFlag());

    const ReferenceComparator<float> *refCmp2 =
        dynamic_cast<const ReferenceComparator<float>* >(cmps[1]);
    ASSERT_TRUE(refCmp2 != NULL);
    ASSERT_EQ(false, refCmp2->getReverseFlag());

    POOL_DELETE_CLASS(cmp);
}

TEST_F(SorterProviderTest, testAddAndGetSortExprMeta) {
    SorterResource resource;
    resource.pool = _pool;
    common::Ha3MatchDocAllocatorPtr allocatorPtr(new common::Ha3MatchDocAllocator(_pool));
    DataProvider dataProvider;
    resource.dataProvider = &dataProvider;
    resource.location = SL_SEARCHER;
    resource.matchDocAllocator = allocatorPtr;
    SorterProvider provider(resource);

    matchdoc::Reference<float> *refer1 = provider.declareVariable<float>("v1", false);
    matchdoc::Reference<float> *refer2 = provider.declareVariable<float>("v2", false);

    provider.addSortExprMeta("v1", refer1, true);
    provider.addSortExprMeta("v2", refer2, false);

    const vector<SortExprMeta>& sortExprMetas = provider.getSortExprMeta();
    ASSERT_EQ(size_t(2), sortExprMetas.size());
    ASSERT_EQ(string("v1"), sortExprMetas[0].sortExprName);
    ASSERT_EQ((const matchdoc::ReferenceBase*)refer1,
                         sortExprMetas[0].sortRef);
    ASSERT_EQ(true, sortExprMetas[0].sortFlag);

    ASSERT_EQ(string("v2"), sortExprMetas[1].sortExprName);
    ASSERT_EQ((const matchdoc::ReferenceBase*)refer2, sortExprMetas[1].sortRef);
    ASSERT_EQ(false, sortExprMetas[1].sortFlag);
}

TEST_F(SorterProviderTest, testDeclareVariable) {
    SorterResource resource;
    resource.pool = _pool;
    common::Ha3MatchDocAllocatorPtr allocatorPtr(new common::Ha3MatchDocAllocator(_pool));
    DataProvider dataProvider;
    resource.dataProvider = &dataProvider;
    resource.location = SL_SEARCHER;
    resource.matchDocAllocator = allocatorPtr;
    innerTestDeclareReference(resource);
    allocatorPtr->extend();
    autil::DataBuffer dataBuffer;
    std::vector<matchdoc::MatchDoc> matchdocs;
    allocatorPtr->serialize(dataBuffer, matchdocs, SL_QRS);
    common::Ha3MatchDocAllocatorPtr proxyAllocatorPtr(new common::Ha3MatchDocAllocator(_pool));
    std::vector<matchdoc::MatchDoc> newMatchdocs;
    proxyAllocatorPtr->deserialize(dataBuffer, newMatchdocs);
    DataProvider dataProvider1;
    resource.dataProvider = &dataProvider1;
    resource.matchDocAllocator = proxyAllocatorPtr;
    resource.location = SL_PROXY;
    innerTestDeclareReference(resource);
}

void SorterProviderTest::innerTestDeclareReference(const SorterResource &resource) {
    SorterProvider provider(resource);
    matchdoc::Reference<float> *refer = provider.declareVariable<float>("test_no_serialize", false);
    ASSERT_TRUE(refer != NULL);
    ASSERT_EQ(SL_NONE, refer->getSerializeLevel());

    matchdoc::Reference<float> *refer1 = provider.declareVariable<float>("test", true);
    ASSERT_TRUE(refer1 != NULL);
    ASSERT_EQ(SL_VARIABLE, refer1->getSerializeLevel());
    matchdoc::Reference<float> *refer2;
    refer2 = provider.findVariableReference<float>("test");
    ASSERT_TRUE(refer1 == refer2);

    refer2 = provider.findVariableReference<float>("not_exist");
    ASSERT_TRUE(refer2 == NULL);

    matchdoc::Reference<int> *refer3 = provider.findVariableReference<int>("test");
    ASSERT_TRUE(refer3 == NULL);
}

TEST_F(SorterProviderTest, testDeclareGlobalVariable) {
    SorterResource resource;
    resource.pool = _pool;
    common::Ha3MatchDocAllocatorPtr allocatorPtr(new common::Ha3MatchDocAllocator(_pool));
    DataProvider dataProvider;
    resource.dataProvider = &dataProvider;
    resource.location = SL_SEARCHER;
    resource.matchDocAllocator = allocatorPtr;
    innerTestGlobalDeclareReference(resource);

    AttributeItemMapPtr attrItemMapPtr = dataProvider.getNeedSerializeGlobalVariables();
    vector<AttributeItemMapPtr> attrItemMapPtrVec;
    attrItemMapPtrVec.push_back(attrItemMapPtr);

    resource.dataProvider = &dataProvider;
    resource.location = SL_PROXY;
    resource.globalVariables = &attrItemMapPtrVec;
    innerTestGlobalDeclareReference(resource);
}

void SorterProviderTest::innerTestGlobalDeclareReference(const SorterResource &resource) {
    SorterProvider provider(resource);
    float *v = provider.declareGlobalVariable<float>("test_no_serialize", false);
    if (resource.location == SL_PROXY) {
        ASSERT_TRUE(v == NULL);
    } else {
        ASSERT_TRUE(v != NULL);
    }

    float *v1 = provider.declareGlobalVariable<float>("test", true);
    if (resource.location == SL_PROXY) {
        ASSERT_TRUE(v1 == NULL);
    } else {
        ASSERT_TRUE(v1 != NULL);
    }
    if (resource.location == SL_SEARCHER) {
        AttributeItemMapPtr attrItemMapPtr = resource.dataProvider->getNeedSerializeGlobalVariables();
        ASSERT_EQ(size_t(1), attrItemMapPtr->size());
        ASSERT_TRUE(attrItemMapPtr->find("test") != attrItemMapPtr->end());
        float *v2 = provider.findGlobalVariableInSort<float>("test");
        ASSERT_TRUE(v1 == v2);

        v2 = provider.findGlobalVariableInSort<float>("not_exist");
        ASSERT_TRUE(v2 == NULL);

        int *v3 = provider.findGlobalVariableInSort<int>("test");
        ASSERT_TRUE(v3 == NULL);
    } else {
        // TODO not support
        // vector<float*> v2 = provider.findGlobalVariableInMerge<float>("test");
        // ASSERT_EQ(size_t(1), v2.size());
        // ASSERT_TRUE(v2[0]);
    }
}

TEST_F(SorterProviderTest, testRequireAttributeInSearcher) {

    static const string tableName = "mainTable";
    static const string indexRootPath = __FILE__;

    Ha3MatchDocAllocatorPtr allocator(new common::Ha3MatchDocAllocator(_pool));
    auto mainSchema = IndexlibPartitionCreator::CreateSchema(
            tableName,
            "pk:string;price:int32;uid:int32", // fields
            "pk:primarykey64:pk;", //indexs
            "price;uid;", //attributes
            "", // summarys
            ""); // truncateProfile

    string docStrs = "cmd=add,pk=pk0,uid=1,price=3;cmd=add,pk=pk1,uid=2,price=4;";

    auto indexPartition = IndexlibPartitionCreator::CreateIndexPartition(
            mainSchema, indexRootPath + tableName, docStrs);
    string schemaName = indexPartition->GetSchema()->GetSchemaName();
    IE_NAMESPACE(partition)::IndexApplication::IndexPartitionMap indexPartitionMap;
    indexPartitionMap.insert(make_pair(schemaName, indexPartition));

    IE_NAMESPACE(partition)::IndexApplicationPtr indexApp(new IndexApplication);
    indexApp->Init(indexPartitionMap, JoinRelationMap());
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr snapshotPtr = indexApp->CreateSnapshot();

    VirtualAttributes virtualAttributes;
    auto tableInfoPtr = TableInfoConfigurator::createFromIndexApp(tableName, indexApp);
    AttributeExpressionCreatorPtr attrExprCreator(
            new AttributeExpressionCreator(_pool,
                    allocator.get(), "mainTable", snapshotPtr.get(), tableInfoPtr,
                    virtualAttributes, nullptr, CavaPluginManagerPtr(), nullptr));

    FakeExpression *fakeExpression = POOL_NEW_CLASS(_pool, FakeExpression, "fakeexpr1");
    FakeExpression *fakeExpression2 = POOL_NEW_CLASS(_pool, FakeExpression, "fakeexpr2");
    attrExprCreator->addPair("fakeexpr1", fakeExpression);
    attrExprCreator->addPair("fakeexpr2", fakeExpression2);
    DataProvider dataProvider;
    SorterResource resource;
    resource.attrExprCreator = attrExprCreator.get();
    resource.matchDocAllocator = allocator;
    resource.location = SL_SEARCHER;
    resource.partitionReaderSnapshot = snapshotPtr.get();
    resource.dataProvider = &dataProvider;
    resource.pool = _pool;

    SorterProvider provider(resource);
    const matchdoc::Reference<uint32_t> *ref = provider.requireAttribute<uint32_t>("uid");
    ASSERT_TRUE(ref == NULL);
    ASSERT_TRUE(provider.getNeedEvaluateAttrs().size() == 0);

    const matchdoc::ReferenceBase *ref2 = provider.requireAttributeWithoutType("uid");
    ASSERT_TRUE(ref2);
    ASSERT_EQ(SL_CACHE, ref2->getSerializeLevel());
    ASSERT_TRUE(provider.getNeedEvaluateAttrs().size() == 1);

    const matchdoc::Reference<int32_t> *ref3 =
        provider.requireAttribute<int32_t>("price", false);
    ASSERT_TRUE(ref3);
    ASSERT_EQ(SL_NONE, ref3->getSerializeLevel());
    ASSERT_TRUE(provider.getNeedEvaluateAttrs().size() == 2);

    ref2 = provider.requireAttributeWithoutType("fakeexpr1");
    ASSERT_TRUE(ref2 != NULL);

    ref2 = provider.requireAttributeWithoutType("fakeexpr2");
    ASSERT_TRUE(ref2 != NULL);

    PoolVector<matchdoc::MatchDoc> matchDocs(_pool);
    matchdoc::MatchDoc matchDoc = (matchdoc::MatchDoc)resource.matchDocAllocator->allocate();
    matchDoc.setDocId(0);
    matchdoc::MatchDoc matchDoc2 = (matchdoc::MatchDoc)resource.matchDocAllocator->allocate();
    matchDoc2.setDocId(1);
    matchDocs.push_back(matchDoc);
    matchDocs.push_back(matchDoc2);
    fakeExpression->setEvaluated();

    provider.prepareForSort(matchDocs);
    ASSERT_EQ(uint32_t(0), fakeExpression->getEvaluateTimes());
    ASSERT_EQ(uint32_t(2),fakeExpression2->getEvaluateTimes());

    ASSERT_EQ(int32_t(3), ref3->getReference(matchDoc));
    ASSERT_EQ(int32_t(4), ref3->getReference(matchDoc2));

    resource.matchDocAllocator->deallocate(matchDoc);
    resource.matchDocAllocator->deallocate(matchDoc2);
}

TEST_F(SorterProviderTest, testRequireAttributeInProxy) {
    SorterResource resource;
    resource.matchDocAllocator.reset(new common::Ha3MatchDocAllocator(_pool));
    resource.location = SL_PROXY;

    resource.matchDocAllocator->declare<uint32_t>("uid");
    resource.matchDocAllocator->declare<int32_t>("price");
    resource.matchDocAllocator->declare<uint32_t>("uid2");

    SorterProvider provider(resource);
    VirtualAttributeClause *clause = new VirtualAttributeClause();
    clause->setOriginalString("virtual_attribute=uid2:uid");
    AtomicSyntaxExpr *expr = new AtomicSyntaxExpr("uid2", vt_uint32, ATTRIBUTE_NAME);
    clause->addVirtualAttribute("uid2", expr);
    provider._convertor.initVirtualAttrs(clause->getVirtualAttributes());

    const matchdoc::ReferenceBase* ref = provider.requireAttributeWithoutType("uid");
    ASSERT_TRUE(ref != NULL);
    ref = provider.requireAttributeWithoutType("uid2");
    ASSERT_TRUE(ref != NULL);
    ref = provider.requireAttributeWithoutType("uid2");
    ASSERT_TRUE(ref != NULL);
    ref = provider.requireAttributeWithoutType("price");
    ASSERT_TRUE(ref != NULL);

    delete clause;
}

TEST_F(SorterProviderTest, testInitTracerRefer) {
    ConfigClause *configClause = new ConfigClause();
    configClause->setRankTrace("DEBUG");

    Request *request = new Request();
    unique_ptr<Request> requestPtr(request);
    request->setConfigClause(configClause);

    common::Ha3MatchDocAllocator *allocator = new common::Ha3MatchDocAllocator(_pool);
    common::Ha3MatchDocAllocatorPtr allocatorPtr(allocator);
    DataProvider dataProvider;

    SorterResource sorterResource;
    sorterResource.pool = _pool;
    sorterResource.request = request;
    sorterResource.dataProvider = &dataProvider;
    sorterResource.matchDocAllocator = allocatorPtr;

    SorterProvider sorterProvider(sorterResource);

    ASSERT_TRUE(sorterProvider.getTracerRefer());
    ASSERT_EQ(ISEARCH_TRACE_DEBUG, sorterProvider.getTracerLevel());
}

TEST_F(SorterProviderTest, testAddNeedEvaluateExpression) {
    SorterResource sorterResource;
    SorterProvider provider(sorterResource);

    RankAttributeExpression rankExpr(new TestScorer(), nullptr);
    ConstAttributeExpression<int32_t> constExpr(1);

    provider.addNeedEvaluateExpression(&rankExpr);
    provider.addNeedEvaluateExpression(&constExpr);

    const vector<AttributeExpression*> &exprs =
        provider.getNeedEvaluateAttrs();
    ASSERT_EQ((size_t)1, exprs.size());
    ASSERT_EQ((AttributeExpression*)&constExpr, exprs[0]);
}

TEST_F(SorterProviderTest, testCreateSortExpressionFromSubDocAttributeName) {

    static const string tableName = "mainTable";
    static const string indexRootPath = __FILE__;

    Ha3MatchDocAllocatorPtr allocator(new common::Ha3MatchDocAllocator(_pool, true));
    auto mainSchema = IndexlibPartitionCreator::CreateSchema(
            tableName,
            "pk:string;price:int32;", // fields
            "pk:primarykey64:pk;", //indexs
            "price", //attributes
            "", // summarys
            ""); // truncateProfile
    auto subSchema = IndexlibPartitionCreator::CreateSchema(
            tableName,
            "sub_pk:string;sub_price:int32;", // fields
            "sub_pk:primarykey64:sub_pk;", //indexs
            "sub_price", //attributes
            "", // summarys
            ""); // truncateProfile
    string docStrs = "cmd=add,pk=pk0,price=11,sub_pk=21,sub_price=31;"
                     "cmd=add,pk=pk1,price=12,sub_pk=22,sub_price=32;";

    auto indexPartition = IndexlibPartitionCreator::CreateIndexPartition(
            IndexlibPartitionCreator::CreateSchemaWithSub(mainSchema, subSchema),
            indexRootPath + tableName, docStrs);
    string schemaName = indexPartition->GetSchema()->GetSchemaName();
    IE_NAMESPACE(partition)::IndexApplication::IndexPartitionMap indexPartitionMap;
    indexPartitionMap.insert(make_pair(schemaName, indexPartition));

    IE_NAMESPACE(partition)::IndexApplicationPtr indexApp(new IndexApplication);
    indexApp->Init(indexPartitionMap, JoinRelationMap());
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr snapshotPtr = indexApp->CreateSnapshot();

    VirtualAttributes virtualAttributes;
    auto tableInfoPtr = TableInfoConfigurator::createFromIndexApp(tableName, indexApp);
    AttributeExpressionCreatorPtr attrExprCreator(
            new AttributeExpressionCreator(_pool,
                    allocator.get(), "mainTable", snapshotPtr.get(), tableInfoPtr,
                    virtualAttributes, nullptr, CavaPluginManagerPtr(), nullptr));

    Request request;
    ConfigClause *configClause = new ConfigClause();
    configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_GROUP);
    request.setConfigClause(configClause);
    common::MultiErrorResultPtr errorResult(new MultiErrorResult);
    DataProvider dataProvider;

    SortExpressionCreator sortExprCreater(attrExprCreator.get(), nullptr,
            allocator.get(), errorResult, _pool);

    SorterResource sorterResource;
    sorterResource.attrExprCreator = attrExprCreator.get();
    sorterResource.pool = _pool;
    sorterResource.sortExprCreator = &sortExprCreater;
    sorterResource.request = &request;
    sorterResource.dataProvider = &dataProvider;
    sorterResource.matchDocAllocator = allocator;
    sorterResource.location = SL_SEARCHER;
    sorterResource.partitionReaderSnapshot = snapshotPtr.get();
    {
        SorterProvider provider(sorterResource);
        ASSERT_TRUE(!provider.createSortExpression("sub_price", true));
        ASSERT_TRUE(provider.createSortExpression("price", true));
    }
    {
        configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_FLAT);
        SorterProvider provider(sorterResource);
        ASSERT_TRUE(provider.createSortExpression("sub_price", true));

    }
}

END_HA3_NAMESPACE(sorter);
