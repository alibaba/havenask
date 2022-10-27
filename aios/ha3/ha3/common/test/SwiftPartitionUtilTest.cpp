#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/SwiftPartitionUtil.h>

USE_HA3_NAMESPACE(proto);

BEGIN_HA3_NAMESPACE(common);

class SwiftPartitionUtilTest : public TESTBASE {
public:
    SwiftPartitionUtilTest();
    ~SwiftPartitionUtilTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, SwiftPartitionUtilTest);


SwiftPartitionUtilTest::SwiftPartitionUtilTest() { 
}

SwiftPartitionUtilTest::~SwiftPartitionUtilTest() { 
}

void SwiftPartitionUtilTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SwiftPartitionUtilTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SwiftPartitionUtilTest, testSwiftPartitionToRange) { 
    HA3_LOG(DEBUG, "Begin Test!");
    {
        uint32_t swiftPartId = 10;
        Range range;
        ASSERT_TRUE(SwiftPartitionUtil::swiftPartitionToRange(swiftPartId, range));
        ASSERT_EQ(uint32_t(10), range.from());
        ASSERT_EQ(uint32_t(10), range.to());
    }

    {
        uint32_t swiftPartId = 66666;
        Range range;
        ASSERT_TRUE(!SwiftPartitionUtil::swiftPartitionToRange(swiftPartId, range));
    }
}

TEST_F(SwiftPartitionUtilTest, testRangeToSwiftPartition) {
    {
        Range range;
        range.set_from(100);
        range.set_to(100);
        uint32_t swiftPartId;
        ASSERT_TRUE(SwiftPartitionUtil::rangeToSwiftPartition(range, swiftPartId));
        ASSERT_EQ(uint32_t(100), swiftPartId);
    }

    {
        Range range;
        range.set_from(100);
        range.set_to(99);
        uint32_t swiftPartId;
        ASSERT_TRUE(!SwiftPartitionUtil::rangeToSwiftPartition(range, swiftPartId));
    }    
}

END_HA3_NAMESPACE(common);

