#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/RangeSet.h>

BEGIN_HA3_NAMESPACE(common);

class RangeSetTest : public TESTBASE {
public:
    RangeSetTest();
    ~RangeSetTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, RangeSetTest);


RangeSetTest::RangeSetTest() { 
}

RangeSetTest::~RangeSetTest() { 
}

void RangeSetTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void RangeSetTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(RangeSetTest, testPushEmpty)
{
    RangeSet set;
    ASSERT_TRUE(set.push(0,2));
    ASSERT_TRUE(set._map.find(0) != set._map.end());
    ASSERT_EQ((uint32_t)2, set._map[0]);
}

TEST_F(RangeSetTest, testPushInvalidElement)
{
    RangeSet set;
    ASSERT_TRUE(!set.push(2,1));
}

TEST_F(RangeSetTest, testPushWithOneElement)
{
#define TWO_SUCESS(a,b,c,d){                                    \
        RangeSet set;                                           \
        ASSERT_TRUE(set.push((a),(b)));                      \
        ASSERT_TRUE(set._map.find((a)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(b), set._map[(a)]);     \
        ASSERT_TRUE(set.push((c),(d)));                      \
        ASSERT_TRUE(set._map.find((a)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(b), set._map[(a)]);     \
        ASSERT_TRUE(set._map.find((c)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(d), set._map[(c)]);     \
    }
    
    TWO_SUCESS(5,7,2,3);
    TWO_SUCESS(2,3,5,7);
#define ONE_SUCESS(a,b,c,d,e,f){                                \
        RangeSet set;                                           \
        ASSERT_TRUE(set.push((a),(b)));                      \
        ASSERT_TRUE(set._map.find((a)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(b), set._map[(a)]);     \
        ASSERT_TRUE(set.push((c),(d)));                      \
        ASSERT_EQ((size_t)(1), set._map.size());     \
        ASSERT_TRUE(set._map.find((e)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(f), set._map[(e)]);     \
    }
    ONE_SUCESS(5,7,3,4,3,7);
    ONE_SUCESS(5,7,3,5,3,7);
    ONE_SUCESS(5,7,8,9,5,9);
    ONE_SUCESS(5,7,7,9,5,9);

    ONE_SUCESS(5,7,3,6,3,7);
    ONE_SUCESS(5,7,6,9,5,9);
    
    ONE_SUCESS(5,7,3,9,3,9);
    ONE_SUCESS(5,7,5,9,5,9);
    ONE_SUCESS(5,7,4,7,4,7);
#define COVER_FAILED(a,b,c,d){                                  \
        RangeSet set;                                           \
        ASSERT_TRUE(set.push((a),(b)));                      \
        ASSERT_TRUE(set._map.find((a)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(b), set._map[(a)]);     \
        ASSERT_TRUE(!set.push((c),(d)));                     \
        ASSERT_EQ((size_t)(1), set._map.size());     \
        ASSERT_TRUE(set._map.find((a)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(b), set._map[(a)]);     \
    }
    COVER_FAILED(1,3,1,3);
    COVER_FAILED(1,3,1,2);
    COVER_FAILED(1,3,2,3);
    COVER_FAILED(1,3,1,1);
    COVER_FAILED(1,3,3,3);
    COVER_FAILED(1,3,2,2);
    COVER_FAILED(1,9,2,7);
}

TEST_F(RangeSetTest, testPushWithTwoElements) {
#define THREE_SUCESS(a,b,c,d,e,f){                              \
        RangeSet set;                                           \
        ASSERT_TRUE(set.push((a),(b)));                      \
        ASSERT_TRUE(set.push((c),(d)));                      \
        ASSERT_TRUE(set.push((e),(f)));                      \
        ASSERT_EQ((size_t)(3), set._map.size());     \
        ASSERT_TRUE(set._map.find((a)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(b), set._map[(a)]);     \
        ASSERT_TRUE(set._map.find((c)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(d), set._map[(c)]);     \
        ASSERT_TRUE(set._map.find((e)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(f), set._map[(e)]);     \
    }
    THREE_SUCESS(5,7,9,10,2,3);
    THREE_SUCESS(5,7,9,10,12,12);
    THREE_SUCESS(5,7,15,17,9,12);
    
#define REST_TWO_SUCESS(a,b,c,d,e,f,h,i,j,k){                   \
        RangeSet set;                                           \
        ASSERT_TRUE(set.push((a),(b)));                      \
        ASSERT_TRUE(set.push((c),(d)));                      \
        ASSERT_TRUE(set.push((e),(f)));                      \
        ASSERT_EQ((size_t)(2), set._map.size());     \
        ASSERT_TRUE(set._map.find((h)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(i), set._map[(h)]);     \
        ASSERT_TRUE(set._map.find((j)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(k), set._map[(j)]);     \
    }

    REST_TWO_SUCESS(3,5, 9,10, 1,2, 1,5, 9,10);
    REST_TWO_SUCESS(3,5, 9,10, 1,3, 1,5, 9,10);
    REST_TWO_SUCESS(3,5, 9,10, 1,4, 1,5, 9,10);
    REST_TWO_SUCESS(3,5, 9,10, 1,5, 1,5, 9,10);
    REST_TWO_SUCESS(3,5, 9,10, 1,6, 1,6, 9,10);
    REST_TWO_SUCESS(3,5, 9,10, 3,6, 3,6, 9,10);
    REST_TWO_SUCESS(2,5, 9,10, 5,7, 2,7, 9,10);
    REST_TWO_SUCESS(2,5, 9,10, 6,7, 2,7, 9,10);
    REST_TWO_SUCESS(2,5, 9,10, 7,9, 2,5, 7,10);
    REST_TWO_SUCESS(2,5, 9,10, 7,8, 2,5, 7,10);

#define REST_ONE_SUCESS(a,b,c,d,e,f,h,i){                       \
        RangeSet set;                                           \
        ASSERT_TRUE(set.push((a),(b)));                      \
        ASSERT_TRUE(set.push((c),(d)));                      \
        ASSERT_TRUE(set.push((e),(f)));                      \
        ASSERT_EQ((size_t)(1), set._map.size());     \
        ASSERT_TRUE(set._map.find((h)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(i), set._map[(h)]);     \
    }
    REST_ONE_SUCESS(2,5, 8,10, 6,7, 2,10 );
    REST_ONE_SUCESS(2,5, 8,10, 6,8, 2,10 );
    REST_ONE_SUCESS(2,5, 8,10, 5,7, 2,10 );
    REST_ONE_SUCESS(2,5, 8,10, 5,8, 2,10 );
    REST_ONE_SUCESS(2,5, 8,10, 2,7, 2,10 );
    REST_ONE_SUCESS(2,5, 8,10, 2,8, 2,10 );
    REST_ONE_SUCESS(2,5, 8,10, 2,10, 2,10 );
    REST_ONE_SUCESS(2,5, 8,10, 6,10, 2,10 );
    REST_ONE_SUCESS(2,5, 8,10, 1,11, 1,11 );
    
#define REST_TWO_FAILED(a,b,c,d,e,f){                           \
        RangeSet set;                                           \
        ASSERT_TRUE(set.push((a),(b)));                      \
        ASSERT_TRUE(set.push((c),(d)));                      \
        ASSERT_TRUE(!set.push((e),(f)));                     \
        ASSERT_EQ((size_t)(2), set._map.size());     \
        ASSERT_TRUE(set._map.find((a)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(b), set._map[(a)]);     \
        ASSERT_TRUE(set._map.find((c)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(d), set._map[(c)]);     \
    }
    REST_TWO_FAILED(2,7, 13,17, 15,17);
    REST_TWO_FAILED(2,7, 13,17, 13,17);
    REST_TWO_FAILED(2,7, 13,17, 13,14);
    REST_TWO_FAILED(2,7, 13,17, 14,15);
    REST_TWO_FAILED(2,7, 13,17, 5,7);
    REST_TWO_FAILED(2,7, 13,17, 2,7);
    REST_TWO_FAILED(2,7, 13,17, 2,5);
    REST_TWO_FAILED(2,7, 13,17, 5,6);
}
TEST_F(RangeSetTest, testPushWithMultiElements){
    
#define FOUR_REST_ONE_SUCESS(a,b,c,d,e,f,h,i,j,k){              \
        RangeSet set;                                           \
        ASSERT_TRUE(set.push((a),(b)));                      \
        ASSERT_TRUE(set.push((c),(d)));                      \
        ASSERT_TRUE(set.push((e),(f)));                      \
        ASSERT_TRUE(set.push((h),(i)));                      \
        ASSERT_EQ((size_t)(1), set._map.size());     \
        ASSERT_TRUE(set._map.find((j)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(k), set._map[(j)]);     \
    }
    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 0,11, 0,12);
    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 1,11, 0,12);
    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 2,11, 0,12);
    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 3,11, 0,12);

    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 0,10, 0,12);
    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 1,10, 0,12);
    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 2,10, 0,12);
    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 3,10, 0,12);

    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 0,9, 0,12);
    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 1,9, 0,12);
    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 2,9, 0,12);
    FOUR_REST_ONE_SUCESS(0,2, 5,8, 10,12, 3,9, 0,12);
    
    FOUR_REST_ONE_SUCESS(1,2, 5,8, 10,12, 0,11, 0,12);
    FOUR_REST_ONE_SUCESS(2,3, 5,8, 10,12, 0,11, 0,12);
    FOUR_REST_ONE_SUCESS(1,2, 5,8, 10,12, 0,12, 0,12);
    FOUR_REST_ONE_SUCESS(2,3, 5,8, 10,12, 0,13, 0,13);

    FOUR_REST_ONE_SUCESS(1,2, 5,8, 10,12, 0,10, 0,12);
    FOUR_REST_ONE_SUCESS(2,3, 5,8, 10,12, 0,9, 0,12);

#define FOUR_REST_THREE_FAILED(a,b,c,d,e,f,h,i){                \
        RangeSet set;                                           \
        ASSERT_TRUE(set.push((a),(b)));                      \
        ASSERT_TRUE(set.push((c),(d)));                      \
        ASSERT_TRUE(set.push((e),(f)));                      \
        ASSERT_TRUE(!set.push((h),(i)));                     \
        ASSERT_EQ((size_t)(3), set._map.size());     \
        ASSERT_TRUE(set._map.find((a)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(b), set._map[(a)]);     \
        ASSERT_TRUE(set._map.find((c)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(d), set._map[(c)]);     \
        ASSERT_TRUE(set._map.find((e)) != set._map.end());   \
        ASSERT_EQ((uint32_t)(f), set._map[(e)]);     \
    }

    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 0,0);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 0,1);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 0,2);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 2,2);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 1,2);

    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 5,5);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 5,6);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 5,8);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 6,7);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 7,8);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 8,8);

    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 10,10);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 10,11);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 10,12);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 11,11);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 11,12);
    FOUR_REST_THREE_FAILED(0,2, 5,8, 10,12, 12,12);
}

TEST_F(RangeSetTest, testPushRandom)
{
    RangeSet set;
    for(uint32_t i = 0; i < 65536; ++i) {
        if (0 == i%3) {
            ASSERT_TRUE(set.push(i,i));
        }
    }
    for(uint32_t i = 0; i < 65536; ++i) {
        if (2 == i%3) {
            ASSERT_TRUE(set.push(i,i));
        }
    }
    for(uint32_t i = 0; i < 65536; ++i) {
        if (1 == i%3) {
            ASSERT_TRUE(set.push(i,i));
        }
    }
    ASSERT_EQ((size_t)(1), set._map.size());
    ASSERT_TRUE(set._map.find((0)) != set._map.end());
    ASSERT_EQ((uint32_t)(65535), set._map[0]);   
}

TEST_F(RangeSetTest, testTryPush) {
    RangeSet rangeSet;
    ASSERT_TRUE(rangeSet.push(0, 500));
    ASSERT_TRUE(rangeSet.push(600, 65535));
    
    ASSERT_TRUE(!rangeSet.tryPush(0, 100));
    ASSERT_TRUE(!rangeSet.tryPush(200, 500));
    ASSERT_TRUE(!rangeSet.tryPush(0, 500));
    ASSERT_TRUE(rangeSet.tryPush(300, 501));
    ASSERT_TRUE(rangeSet.tryPush(501, 599));
    ASSERT_TRUE(rangeSet.tryPush(0, 65535));
    ASSERT_TRUE(!rangeSet.tryPush(599, 550));
    ASSERT_TRUE(!rangeSet.tryPush(700, 2000));
    
    ASSERT_EQ(size_t(2), rangeSet._map.size());
    ASSERT_EQ((uint32_t)500, rangeSet._map[0]);
    ASSERT_EQ((uint32_t)65535, rangeSet._map[600]);
}

TEST_F(RangeSetTest, testIsComplete) {
    RangeSet rangeSet;
    ASSERT_TRUE(!rangeSet.isComplete());
    ASSERT_TRUE(rangeSet.push(0, 500));
    ASSERT_TRUE(rangeSet.push(600, 65535));
    ASSERT_TRUE(!rangeSet.isComplete());
    ASSERT_TRUE(rangeSet.push(400, 700));
    ASSERT_TRUE(rangeSet.isComplete());

    ASSERT_TRUE(!rangeSet.push(0, 700));
    ASSERT_TRUE(rangeSet.isComplete());
}

END_HA3_NAMESPACE(common);
