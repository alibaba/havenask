#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/MultiLevelRangeSet.h>

BEGIN_HA3_NAMESPACE(common);

class MultiLevelRangeSetTest : public TESTBASE {
public:
    MultiLevelRangeSetTest();
    ~MultiLevelRangeSetTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, MultiLevelRangeSetTest);


MultiLevelRangeSetTest::MultiLevelRangeSetTest() { 
}

MultiLevelRangeSetTest::~MultiLevelRangeSetTest() { 
}

void MultiLevelRangeSetTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void MultiLevelRangeSetTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(MultiLevelRangeSetTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    MultiLevelRangeSet mlrs(3);
    ASSERT_TRUE(mlrs.push(0, 100));
    ASSERT_TRUE(mlrs.tryPush(0, 100));
    ASSERT_TRUE(mlrs.push(0, 100));
    ASSERT_TRUE(mlrs.tryPush(0, 100));
    ASSERT_TRUE(mlrs.push(0, 100));
    ASSERT_TRUE(!mlrs.tryPush(0, 100));
    ASSERT_TRUE(!mlrs.isComplete());

    ASSERT_TRUE(mlrs.push(99, 65535));
    ASSERT_TRUE(mlrs.push(99, 65535));
    ASSERT_TRUE(mlrs.push(99, 65535));

    ASSERT_TRUE(!mlrs.tryPush(1000, 2000));
    ASSERT_TRUE(mlrs.isComplete());
}

END_HA3_NAMESPACE(common);

