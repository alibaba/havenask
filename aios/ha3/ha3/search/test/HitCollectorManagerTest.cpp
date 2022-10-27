#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/HitCollectorManager.h>
#include <ha3/search/SortExpressionCreator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/ReferenceComparator.h>
#include <ha3/rank/MultiHitCollector.h>
#include <ha3/rank/ExpressionComparator.h>
#include <ha3/search/test/FakeAttributeExpressionFactory.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(rank);
BEGIN_HA3_NAMESPACE(search);

class HitCollectorManagerTest : public TESTBASE {
public:
    HitCollectorManagerTest();
    ~HitCollectorManagerTest();
public:
    void setUp();
    void tearDown();
protected:
    AttributeExpressionCreator *_attrExprCreator;
    SortExpressionCreator *_sortExprCreator;
    common::Ha3MatchDocAllocatorPtr _allocator;
    autil::mem_pool::Pool *_pool;
    common::MultiErrorResultPtr _errorResult;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, HitCollectorManagerTest);


HitCollectorManagerTest::HitCollectorManagerTest() { 
    _pool = new autil::mem_pool::Pool(1024);
}

HitCollectorManagerTest::~HitCollectorManagerTest() { 
    _allocator.reset();
    if (_pool) {
        delete _pool;
        _pool = NULL;
    }    
}

void HitCollectorManagerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    FakeAttributeExpressionFactory * fakeFactory =
        POOL_NEW_CLASS(_pool, FakeAttributeExpressionFactory,
                       "uid", "1, 2, 3, 4, 5, 6, 7, 8, 9, 10", _pool);
    fakeFactory->setInt32Attribute("uid1", "11, 12, 13, 14, 15, 16, 17, 18, 19, 20");
    fakeFactory->setInt32Attribute("uid2", "21, 22, 23, 24, 25, 26, 27, 28, 29, 30");
    fakeFactory->setInt32Attribute("uid3", "31, 32, 33, 34, 35, 36, 37, 38, 39, 40");
    
    _attrExprCreator = new FakeAttributeExpressionCreator(_pool,
            IndexPartitionReaderWrapperPtr(), NULL, NULL, NULL, NULL, NULL);
    _attrExprCreator->resetAttrExprFactory(fakeFactory);
    _allocator.reset(new common::Ha3MatchDocAllocator(_pool)); 
    _sortExprCreator = new SortExpressionCreator(_attrExprCreator, NULL, 
            _allocator.get(), common::MultiErrorResultPtr(), _pool);
}

void HitCollectorManagerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    if (_attrExprCreator) {
        delete _attrExprCreator;
        _attrExprCreator = NULL;
    }
    
    if (_sortExprCreator) {
        delete _sortExprCreator;
        _sortExprCreator = NULL;
    }
}

TEST_F(HitCollectorManagerTest, testInit) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    string query = "query=phrase:with"
                   "&&rank_sort=sort:uid1#uid2,percent:50;sort:uid1,percent:50;";
    vector<SortExpressionVector> rankSortExpressions;
    rankSortExpressions.resize(2);
    AttributeExpression* ae1 = _attrExprCreator->createAtomicExpr("uid1");
    ae1->allocate(_allocator.get());
    SortExpression* se1 = _sortExprCreator->createSortExpression(ae1, false);
    AttributeExpression* ae2 = _attrExprCreator->createAtomicExpr("uid2");
    ae2->allocate(_allocator.get());
    SortExpression* se2 = _sortExprCreator->createSortExpression(ae2, false);
    rankSortExpressions[0].push_back(se1);
    rankSortExpressions[0].push_back(se2);
    rankSortExpressions[1].push_back(se1);
    _sortExprCreator->setRankSortExpressions(rankSortExpressions);
    
    RequestPtr requestPtr = RequestCreator::prepareRequest(query);
    rank::ComparatorCreator comparatorCreator(_pool, 
            requestPtr->getConfigClause()->isOptimizeComparator());
    HitCollectorManager hitCollectorManager(_attrExprCreator, _sortExprCreator,
            _pool, &comparatorCreator, _allocator, _errorResult);
    uint32_t topK = 10;
    ASSERT_TRUE(hitCollectorManager.init(requestPtr.get(), topK));

    MultiHitCollector* multiHitCollector = dynamic_cast<MultiHitCollector*>(
            hitCollectorManager.getRankHitCollector());
    ASSERT_TRUE(multiHitCollector);
    const vector<HitCollectorBase*> hitCollectors = 
        multiHitCollector->getHitCollectors();
    ASSERT_EQ(size_t(2), hitCollectors.size());
    HitCollector* hitCollector = dynamic_cast<HitCollector*>(hitCollectors[0]);
    ASSERT_TRUE(hitCollector);
    const ComboComparator* comparator = hitCollector->getComparator();
    const ComboComparator::ComparatorVector &compartorVec1 = 
        comparator->getComparators();

    ASSERT_EQ(size_t(0), compartorVec1.size());
    ASSERT_EQ("ref_expr", comparator->getType());

    hitCollector = dynamic_cast<HitCollector*> (hitCollectors[1]);
    ASSERT_TRUE(hitCollector);
    comparator = hitCollector->getComparator();
    const ComboComparator::ComparatorVector &compartorVec2 = 
        comparator->getComparators();
    ASSERT_EQ(size_t(0), compartorVec2.size());
    ASSERT_EQ("one_ref", comparator->getType());
    auto oneRefComp = 
        dynamic_cast<const OneRefComparatorTyped<int32_t> * >(comparator);
    ASSERT_TRUE(oneRefComp);
}

END_HA3_NAMESPACE(search);

