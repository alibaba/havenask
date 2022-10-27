#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/GradeCalculator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <matchdoc/MatchDoc.h>

using namespace std;
BEGIN_HA3_NAMESPACE(rank);

class GradeCalculatorTest : public TESTBASE {
public:
    GradeCalculatorTest();
    ~GradeCalculatorTest();
public:
    void setUp();
    void tearDown();
protected:
    matchdoc::MatchDoc _matchDoc;
    matchdoc::Reference<int32_t> *_scoreRef;
    GradeCalculatorBase *_gradeCalculator;
    autil::mem_pool::Pool *_pool;
    common::Ha3MatchDocAllocator *_allocator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, GradeCalculatorTest);


GradeCalculatorTest::GradeCalculatorTest() { 
    _pool = new autil::mem_pool::Pool(1024);
    _allocator = new common::Ha3MatchDocAllocator(_pool);
}

GradeCalculatorTest::~GradeCalculatorTest() { 
    delete _allocator;
    delete _pool;
}

void GradeCalculatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _scoreRef = _allocator->declare<int32_t>("score");
    _gradeCalculator = new GradeCalculator<int32_t>(_scoreRef);
    _matchDoc = _allocator->allocate(0);
}

void GradeCalculatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    _allocator->deallocate(_matchDoc);
    delete _gradeCalculator;
}

TEST_F(GradeCalculatorTest, testCalculate) { 
    HA3_LOG(DEBUG, "Begin Test!");

    DistinctInfo distInfo;
    distInfo.setMatchDoc(_matchDoc);

    _scoreRef->set(_matchDoc, 3);
    _gradeCalculator->calculate(&distInfo);
    ASSERT_EQ((uint32_t)0, distInfo.getGradeBoost());

    vector<double> gradeThresholds;
    gradeThresholds.push_back(3.0);
    gradeThresholds.push_back(5.0);
    _gradeCalculator->setGradeThresholds(gradeThresholds);

    _scoreRef->set(_matchDoc, 0);
    _gradeCalculator->calculate(&distInfo);
    ASSERT_EQ((uint32_t)0, distInfo.getGradeBoost());

    _scoreRef->set(_matchDoc, 3);
    _gradeCalculator->calculate(&distInfo);
    ASSERT_EQ((uint32_t)1, distInfo.getGradeBoost());

    _scoreRef->set(_matchDoc, 4);
    _gradeCalculator->calculate(&distInfo);
    ASSERT_EQ((uint32_t)1, distInfo.getGradeBoost());

    _scoreRef->set(_matchDoc, 5);
    _gradeCalculator->calculate(&distInfo);
    ASSERT_EQ((uint32_t)2, distInfo.getGradeBoost());
    
    _scoreRef->set(_matchDoc, 8);
    _gradeCalculator->calculate(&distInfo);
    ASSERT_EQ((uint32_t)2, distInfo.getGradeBoost());
}

END_HA3_NAMESPACE(rank);

