#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/PageDistinctHitConvertor.h>
#include <ha3/qrs/test/PageDistinctSelectorTest.h>

using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);

class PageDistinctHitConvertorTest : public TESTBASE {
public:
    PageDistinctHitConvertorTest();
    ~PageDistinctHitConvertorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, PageDistinctHitConvertorTest);


PageDistinctHitConvertorTest::PageDistinctHitConvertorTest() { 
}

PageDistinctHitConvertorTest::~PageDistinctHitConvertorTest() { 
}

void PageDistinctHitConvertorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void PageDistinctHitConvertorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

#define CHECK_PAGE_DIST_HIT(hitPos, grade, key, hasKey, pageDistHit) {  \
        ASSERT_TRUE(pageDistHit);                                    \
        ASSERT_EQ(hasKey, pageDistHit->hasKeyValue);         \
        if (hasKey) {                                                   \
            ASSERT_EQ((uint32_t)key, pageDistHit->keyValue); \
        }                                                               \
        ASSERT_EQ((size_t)hitPos, pageDistHit->pos);         \
        ASSERT_EQ((int32_t)grade, pageDistHit->gradeValue); }

TEST_F(PageDistinctHitConvertorTest, testConvertAndSort) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    string distKey = "dist_key";
    vector<bool> sortFlags;
    vector<double> grades;
    PageDistinctHitConvertor<uint32_t> convertor1(distKey, sortFlags, grades);
    
    vector<HitPtr> hitVec = 
        PageDistinctSelectorTest::createHits(distKey,
                "1, 1, 0.7;"
                "2, 2, 0.9");
    
    vector<PageDistinctHit<uint32_t>*> pageDistHits;
    convertor1.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)2, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(1, 0, 2, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[1]);

    sortFlags.clear();
    sortFlags.resize(1, true);
    PageDistinctHitConvertor<uint32_t> convertor2(distKey, sortFlags, grades);
    convertor2.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)2, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(1, 0, 2, true, pageDistHits[1]);    

    sortFlags.clear();
    grades.resize(1, 0.8);
    PageDistinctHitConvertor<uint32_t> convertor3(distKey, sortFlags, grades);
    convertor3.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)2, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(1, 1, 2, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[1]);

    sortFlags.clear();
    sortFlags.resize(1, true);
    grades.resize(1, 0.8);
    PageDistinctHitConvertor<uint32_t> convertor4(distKey, sortFlags, grades);
    convertor4.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)2, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(1, -1, 2, true, pageDistHits[1]);

    sortFlags.clear();
    sortFlags.resize(2, false);
    grades.clear();
    PageDistinctHitConvertor<uint32_t> convertor5(distKey, sortFlags, grades);
    convertor5.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)2, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(1, 0, 2, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[1]);

    sortFlags.clear();
    sortFlags.resize(2, false);
    sortFlags[1] = true;
    grades.clear();
    PageDistinctHitConvertor<uint32_t> convertor6(distKey, sortFlags, grades);
    convertor6.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)2, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(1, 0, 2, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[1]);
}

TEST_F(PageDistinctHitConvertorTest, testConvertAndSortWithMultSortValue) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    string distKey = "dist_key";
    vector<bool> sortFlags;
    vector<double> grades;
    PageDistinctHitConvertor<uint32_t> convertor1(distKey, sortFlags, grades);
    
    vector<HitPtr> hitVec = 
        PageDistinctSelectorTest::createHits(distKey,
                "1, 1, 0.7#2.1;"
                "2, 2, 0.9#2.0;"
                "3, 3, 0.9#1.0;"
                "4, 4, 0.7#2.1;");
    
    vector<PageDistinctHit<uint32_t>*> pageDistHits;
    convertor1.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)4, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(2, 0, 3, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(1, 0, 2, true, pageDistHits[1]);
    CHECK_PAGE_DIST_HIT(3, 0, 4, true, pageDistHits[2]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[3]);

    sortFlags.clear();
    sortFlags.resize(2, false);
    PageDistinctHitConvertor<uint32_t> convertor2(distKey, sortFlags, grades);
    convertor2.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)4, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(1, 0, 2, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(2, 0, 3, true, pageDistHits[1]);
    CHECK_PAGE_DIST_HIT(3, 0, 4, true, pageDistHits[2]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[3]);

    sortFlags[1] = true;
    PageDistinctHitConvertor<uint32_t> convertor3(distKey, sortFlags, grades);
    convertor3.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)4, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(2, 0, 3, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(1, 0, 2, true, pageDistHits[1]);
    CHECK_PAGE_DIST_HIT(3, 0, 4, true, pageDistHits[2]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[3]);

    sortFlags.clear();
    sortFlags.resize(2, true);
    PageDistinctHitConvertor<uint32_t> convertor4(distKey, sortFlags, grades);
    convertor4.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)4, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(3, 0, 4, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[1]);
    CHECK_PAGE_DIST_HIT(2, 0, 3, true, pageDistHits[2]);
    CHECK_PAGE_DIST_HIT(1, 0, 2, true, pageDistHits[3]);

    sortFlags[1] = false;
    PageDistinctHitConvertor<uint32_t> convertor5(distKey, sortFlags, grades);
    convertor5.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)4, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(3, 0, 4, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[1]);
    CHECK_PAGE_DIST_HIT(1, 0, 2, true, pageDistHits[2]);
    CHECK_PAGE_DIST_HIT(2, 0, 3, true, pageDistHits[3]);

    sortFlags.clear();
    sortFlags.resize(2, false);
    sortFlags[1] = true;
    grades.clear();
    grades.resize(1, 0.8);
    PageDistinctHitConvertor<uint32_t> convertor6(distKey, sortFlags, grades);
    convertor6.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)4, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(2, 1, 3, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(1, 1, 2, true, pageDistHits[1]);
    CHECK_PAGE_DIST_HIT(3, 0, 4, true, pageDistHits[2]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[3]);

    sortFlags.clear();
    sortFlags.resize(2, true);
    sortFlags[1] = false;
    grades.clear();
    grades.resize(1, 0.8);
    PageDistinctHitConvertor<uint32_t> convertor7(distKey, sortFlags, grades);
    convertor7.convertAndSort(hitVec, pageDistHits);
    ASSERT_EQ((size_t)4, pageDistHits.size());
    CHECK_PAGE_DIST_HIT(3, 0, 4, true, pageDistHits[0]);
    CHECK_PAGE_DIST_HIT(0, 0, 1, true, pageDistHits[1]);
    CHECK_PAGE_DIST_HIT(1, -1, 2, true, pageDistHits[2]);
    CHECK_PAGE_DIST_HIT(2, -1, 3, true, pageDistHits[3]);
}

TEST_F(PageDistinctHitConvertorTest, testConvertDistKey) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    HitPtr hitPtr = PageDistinctSelectorTest::createHit(1, 
            "key", 1, "0");
    PageDistinctHitConvertor<uint32_t> convertor("not_exist_key", 
            vector<bool>(), vector<double>());
    PageDistinctHit<uint32_t> pageDistHit;
    convertor.convertDistKey(*hitPtr, &pageDistHit);
    ASSERT_TRUE(!pageDistHit.hasKeyValue);

    PageDistinctHitConvertor<uint32_t> convertor1("key", 
            vector<bool>(), vector<double>());
    PageDistinctHit<uint32_t> pageDistHit1;
    convertor1.convertDistKey(*hitPtr, &pageDistHit1);
    ASSERT_TRUE(pageDistHit1.hasKeyValue);
    ASSERT_EQ((uint32_t)1, pageDistHit1.keyValue);
}

END_HA3_NAMESPACE(qrs);

