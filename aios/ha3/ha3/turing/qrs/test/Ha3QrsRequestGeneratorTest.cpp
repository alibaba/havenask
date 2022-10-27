#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/turing/qrs/Ha3QrsRequestGenerator.h>

using namespace std;
using namespace testing;

USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(turing);

class Ha3QrsRequestGeneratorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, Ha3QrsRequestGeneratorTest);

void Ha3QrsRequestGeneratorTest::setUp() {
}

void Ha3QrsRequestGeneratorTest::tearDown() {
}

TEST_F(Ha3QrsRequestGeneratorTest, getPartId) {
    {//hashId second less than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {1, 99};
        set<int32_t> partIds;
        Ha3QrsRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(0, partIds.size());
    }
    {//hashId first less than ranges,hashId second in ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {1, 150};
        set<int32_t> partIds;
        Ha3QrsRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    {//hashId first greater than ranges,hashId second less than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {100, 200};
        set<int32_t> partIds;
        Ha3QrsRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    {//hashId first greater than ranges,hashId second less than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {120, 120};
        set<int32_t> partIds;
        Ha3QrsRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    {//hashId first in ranges,hashId second greater than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {99, 300};
        set<int32_t> partIds;
        Ha3QrsRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    {//hashId first greater than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {201, 300};
        set<int32_t> partIds;
        Ha3QrsRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(0, partIds.size());
    }
    {//two ranges normal
        RangeVec ranges = {{100, 200}, {400, 600}};
        PartitionRange hashId = {200, 400};
        set<int32_t> partIds;
        Ha3QrsRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1));
    }
    {//two ranges normal
        RangeVec ranges = {{100, 200}, {400, 600}};
        PartitionRange hashId = {201, 420};
        set<int32_t> partIds;
        Ha3QrsRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(1, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(1));
    }
}

TEST_F(Ha3QrsRequestGeneratorTest, getPartIds) {
    {//no hashId query
        RangeVec ranges = {{100, 200}, {400, 600}};
        RangeVec hashIds = {{0, 99}, {300, 399}};
        set<int32_t> partIds = Ha3QrsRequestGenerator::getPartIds(ranges, hashIds);
        ASSERT_EQ(0, partIds.size());
    }
    {//normal hashId
        RangeVec ranges = {{100, 200}, {400, 600}};
        RangeVec hashIds = {{0, 300}, {301, 620}};
        set<int32_t> partIds = Ha3QrsRequestGenerator::getPartIds(ranges, hashIds);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1));
    }
    
}

END_HA3_NAMESPACE();

