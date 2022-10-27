#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/ComboComparator.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/Comparator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/ReferenceComparator.h>
#include <string>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/CommonDef.h>
#include <ha3/test/test.h>

using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(rank);

class ComboComparatorTest : public TESTBASE {
public:
    ComboComparatorTest();
    ~ComboComparatorTest();
public:
    void setUp();
    void tearDown();
protected:
    matchdoc::MatchDoc createMatchDoc(docid_t docid, hashid_t hashid, float score, int32_t id);
protected:
    ComboComparator *_comboCmp;
    ReferenceComparator<int32_t> *_attrCmp;
    ReferenceComparator<float> *_relvCmp;
    matchdoc::MatchDoc _a;
    matchdoc::MatchDoc _b;
    matchdoc::MatchDoc _c;
    matchdoc::MatchDoc _d;
    matchdoc::MatchDoc _e;
    matchdoc::MatchDoc _f;
    common::Ha3MatchDocAllocator *_allocator;
    const matchdoc::Reference<int32_t> *_idRef;
    const matchdoc::Reference<float> *_scoreRef;
    const matchdoc::Reference<common::ExtraDocIdentifier> *_metaRef;
    const matchdoc::Reference<hashid_t> *_hashIdRef;
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, ComboComparatorTest);


ComboComparatorTest::ComboComparatorTest() { 
    _pool = new autil::mem_pool::Pool(1024);
    _allocator = new common::Ha3MatchDocAllocator(_pool);
    _metaRef = _allocator->declare<ExtraDocIdentifier>("extra_doc_identifier", true, true);
    _idRef = _allocator->declare<int32_t>("id");
    _scoreRef = _allocator->declare<float>("score");
    _hashIdRef = _allocator->declare<hashid_t>(HASH_ID_REF,
            common::HA3_GLOBAL_INFO_GROUP, SL_QRS);

}

ComboComparatorTest::~ComboComparatorTest() { 
    delete _allocator;
    delete _pool;
}

void ComboComparatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    
    _comboCmp = POOL_NEW_CLASS(_pool, ComboComparator);
    ReferenceComparator<ExtraDocIdentifier> *docIdCmp = 
        POOL_NEW_CLASS(_pool, ReferenceComparator<ExtraDocIdentifier>, _metaRef);
    _comboCmp->setExtrDocIdComparator(docIdCmp);

    _attrCmp = POOL_NEW_CLASS(_pool, ReferenceComparator<int32_t>, _idRef);
    _relvCmp = POOL_NEW_CLASS(_pool, ReferenceComparator<float>, _scoreRef);

    _a = createMatchDoc(4, 2, 1.3f, 5);
    _b = createMatchDoc(3, 2, 1.3f, 5);
    _c = createMatchDoc(2, 2, 1.0f, 8);
    _d = createMatchDoc(2, 1, 1.0f, 8);
    _e = createMatchDoc(1, 1, 0.6f, 10);
    _f = createMatchDoc(1, 1, 0.2f, 10);
}

void ComboComparatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");

    _allocator->deallocate(_a);
    _allocator->deallocate(_b);
    _allocator->deallocate(_c);
    _allocator->deallocate(_d);
    _allocator->deallocate(_e);
    _allocator->deallocate(_f);
    POOL_DELETE_CLASS(_comboCmp);
}

TEST_F(ComboComparatorTest, testRelevanceFistCompare) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    _comboCmp->addComparator(_relvCmp);
    _comboCmp->addComparator(_attrCmp);
    ASSERT_EQ((uint32_t)2, _comboCmp->getComparatorCount());
    
    ASSERT_TRUE(_comboCmp->compare(_a, _b));
    ASSERT_TRUE(!_comboCmp->compare(_b, _a));
    ASSERT_TRUE(!_comboCmp->compare(_b, _c));
    ASSERT_TRUE(_comboCmp->compare(_c, _b));
    ASSERT_FALSE(_comboCmp->compare(_c, _d));
    ASSERT_FALSE(_comboCmp->compare(_d, _c));
    ASSERT_TRUE(_comboCmp->compare(_f, _e));
    ASSERT_TRUE(!_comboCmp->compare(_e, _f));
}

TEST_F(ComboComparatorTest, testRelevanceFistCompareReverse) { 
    HA3_LOG(DEBUG, "Begin Test!");
    _comboCmp->addComparator(_relvCmp);
    _comboCmp->addComparator(_attrCmp);
    _relvCmp->setReverseFlag(true);
    ASSERT_EQ((uint32_t)2, _comboCmp->getComparatorCount());
    ASSERT_TRUE(_comboCmp->compare(_a, _b));
    ASSERT_TRUE(!_comboCmp->compare(_b, _a));
    ASSERT_TRUE(_comboCmp->compare(_b, _c));
    ASSERT_TRUE(!_comboCmp->compare(_c, _b));
    ASSERT_TRUE(!_comboCmp->compare(_f, _e));
    ASSERT_TRUE(_comboCmp->compare(_e, _f));
}

TEST_F(ComboComparatorTest, testAttributeFistCompare) { 
    HA3_LOG(DEBUG, "Begin Test!");

    _comboCmp->addComparator(_attrCmp);
    _comboCmp->addComparator(_relvCmp);
    ASSERT_EQ((uint32_t)2, _comboCmp->getComparatorCount());
    
    ASSERT_TRUE(_comboCmp->compare(_a, _b));
    ASSERT_TRUE(!_comboCmp->compare(_b, _a));
    ASSERT_TRUE(_comboCmp->compare(_b, _c));
    ASSERT_TRUE(!_comboCmp->compare(_c, _b));
    ASSERT_TRUE(_comboCmp->compare(_f, _e));
    ASSERT_TRUE(!_comboCmp->compare(_e, _f));
}

TEST_F(ComboComparatorTest, testCompareWithVersion) { 
    HA3_LOG(DEBUG, "Begin Test!");
    _comboCmp->addComparator(_relvCmp);
    POOL_DELETE_CLASS(_attrCmp);
    ASSERT_EQ((uint32_t)1, _comboCmp->getComparatorCount());
    ASSERT_TRUE(!_comboCmp->compare(_d, _c));
    ASSERT_TRUE(!_comboCmp->compare(_c, _d));
    _metaRef->getReference(_c).fullIndexVersion = 0;
    _metaRef->getReference(_d).fullIndexVersion = 2;
    
    ASSERT_TRUE(_comboCmp->compare(_d, _c));
    ASSERT_TRUE(!_comboCmp->compare(_c, _d));        
}


TEST_F(ComboComparatorTest, testCompareWithHashId) { 
    HA3_LOG(DEBUG, "Begin Test!");
    _comboCmp->addComparator(_relvCmp);
    POOL_DELETE_CLASS(_attrCmp);
    ASSERT_EQ((uint32_t)1, _comboCmp->getComparatorCount());
    _hashIdRef->set(_c, 1);
    _hashIdRef->set(_d, 2);
    ASSERT_TRUE(!_comboCmp->compare(_d, _c));
    ASSERT_TRUE(!_comboCmp->compare(_c, _d));
    ReferenceComparator<hashid_t> *haIdCmp = 
        POOL_NEW_CLASS(_pool, ReferenceComparator<hashid_t>, _hashIdRef);
    _comboCmp->setExtrHashIdComparator(haIdCmp);
    ASSERT_TRUE(!_comboCmp->compare(_c, _d));
    ASSERT_TRUE(_comboCmp->compare(_d, _c));    
}

matchdoc::MatchDoc ComboComparatorTest::createMatchDoc(docid_t docid, hashid_t hashid,
        float score, int32_t id)
{
    matchdoc::MatchDoc matchDoc = _allocator->allocate(docid);
    _scoreRef->set(matchDoc, score);
    _idRef->set(matchDoc, id);
    _hashIdRef->set(matchDoc, hashid);
    return matchDoc;
}

TEST_F(ComboComparatorTest, testOneRefComparator) { 
    HA3_LOG(DEBUG, "Begin Test!");
    OneRefComparatorTyped<int32_t> comp(_idRef, false); 
    ASSERT_TRUE(comp.compare(_a, _b));
    ASSERT_TRUE(!comp.compare(_b, _a));
    ASSERT_TRUE(comp.compare(_b, _c));
    ASSERT_TRUE(!comp.compare(_c, _b));
    ASSERT_TRUE(comp.compare(_d, _e));
    ASSERT_TRUE(!comp.compare(_e, _d));
    ASSERT_TRUE(!comp.compare(_f, _e));
    ASSERT_TRUE(!comp.compare(_e, _f));
}

TEST_F(ComboComparatorTest, testOneRefComparatorReversed) { 
    HA3_LOG(DEBUG, "Begin Test!");
    OneRefComparatorTyped<int32_t> comp(_idRef, true); 
    ASSERT_TRUE(comp.compare(_a, _b)); // docid will not reversed
    ASSERT_TRUE(!comp.compare(_b, _a));
    ASSERT_TRUE(!comp.compare(_b, _c));
    ASSERT_TRUE(comp.compare(_c, _b));
    ASSERT_TRUE(!comp.compare(_d, _e));
    ASSERT_TRUE(comp.compare(_e, _d));
    ASSERT_TRUE(!comp.compare(_f, _e));
    ASSERT_TRUE(!comp.compare(_e, _f));
}

TEST_F(ComboComparatorTest, testTwoRefComparator) { 
    HA3_LOG(DEBUG, "Begin Test!");
    TwoRefComparatorTyped<float, int32_t> comp(_scoreRef, _idRef, false, false); 
    ASSERT_EQ((uint32_t)0, comp.getComparatorCount());
    ASSERT_TRUE(comp.compare(_a, _b));
    ASSERT_TRUE(!comp.compare(_b, _a));
    ASSERT_TRUE(!comp.compare(_b, _c));
    ASSERT_TRUE(comp.compare(_c, _b));
    ASSERT_FALSE(comp.compare(_c, _d));
    ASSERT_FALSE(comp.compare(_d, _c));
    ASSERT_TRUE(comp.compare(_f, _e));
    ASSERT_TRUE(!comp.compare(_e, _f));
}

TEST_F(ComboComparatorTest, testTwoRefComparatorFistReverse) { 
    HA3_LOG(DEBUG, "Begin Test!");
    TwoRefComparatorTyped<float, int32_t> comp(_scoreRef, _idRef, true, false); 
    ASSERT_EQ((uint32_t)0, comp.getComparatorCount());
    ASSERT_TRUE(comp.compare(_a, _b));
    ASSERT_TRUE(!comp.compare(_b, _a));
    ASSERT_TRUE(comp.compare(_b, _c));
    ASSERT_TRUE(!comp.compare(_c, _b));
    ASSERT_TRUE(!comp.compare(_f, _e));
    ASSERT_TRUE(comp.compare(_e, _f));
}

TEST_F(ComboComparatorTest, testThreeRefComparator) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ThreeRefComparatorTyped<float, int32_t, hashid_t> comp(
            _scoreRef, _idRef, _hashIdRef, false, false, false); 
    ASSERT_EQ((uint32_t)0, comp.getComparatorCount());
    ASSERT_TRUE(comp.compare(_a, _b));
    ASSERT_TRUE(!comp.compare(_b, _a));
    ASSERT_TRUE(!comp.compare(_b, _c));
    ASSERT_TRUE(comp.compare(_c, _b));
    ASSERT_TRUE(!comp.compare(_c, _d));
    ASSERT_TRUE(comp.compare(_d, _c));
    ASSERT_TRUE(comp.compare(_f, _e));
    ASSERT_TRUE(!comp.compare(_e, _f));
}

TEST_F(ComboComparatorTest, testThreeRefComparatorFirstReverse) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ThreeRefComparatorTyped<float, int32_t, hashid_t> comp(
            _scoreRef, _idRef, _hashIdRef, true, false, false); 
    ASSERT_EQ((uint32_t)0, comp.getComparatorCount());
    ASSERT_TRUE(comp.compare(_a, _b));
    ASSERT_TRUE(!comp.compare(_b, _a));
    ASSERT_TRUE(comp.compare(_b, _c));
    ASSERT_TRUE(!comp.compare(_c, _b));
    ASSERT_TRUE(!comp.compare(_c, _d));
    ASSERT_TRUE(comp.compare(_d, _c));
    ASSERT_TRUE(!comp.compare(_f, _e));
    ASSERT_TRUE(comp.compare(_e, _f));
}


END_HA3_NAMESPACE(rank);

