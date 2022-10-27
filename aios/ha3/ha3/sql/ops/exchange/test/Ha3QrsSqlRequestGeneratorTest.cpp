#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/exchange/Ha3QrsSqlRequestGenerator.h>
using namespace std;
using namespace testing;

USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(sql);

class Ha3QrsSqlRequestGeneratorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void Ha3QrsSqlRequestGeneratorTest::setUp() {
}

void Ha3QrsSqlRequestGeneratorTest::tearDown() {
}

TEST_F(Ha3QrsSqlRequestGeneratorTest, genAccessPart) {
    { //access all part
        TableDistribution tableDist;
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { //access one part
        TableDistribution tableDist;
        tableDist.partCnt = 1;
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(1, partIds.size());
        ASSERT_TRUE(*partIds.begin() >= 0 && *partIds.begin() <=3 );
    }
    { //access one part
        TableDistribution tableDist;
        tableDist.partCnt = 1;
        tableDist.hashMode._hashFields.push_back("metric");
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(1, partIds.size());
        ASSERT_TRUE(*partIds.begin() >= 0 && *partIds.begin() <=3 );
    }

    {// func name is empty
        TableDistribution tableDist;
        tableDist.hashValues["metric"].push_back("sys.cpu");
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    {// hash values not found
        TableDistribution tableDist;
        tableDist.hashValues["metric1"].push_back("1");
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    {// hash func error
        TableDistribution tableDist;
        tableDist.hashValues["metric"] = {"0", "65535"};
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH_ERROR";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    {
        TableDistribution tableDist;
        tableDist.hashValues["metric"] = {"0", "65535"};
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 3));
    }
    {// with hash value sep
        TableDistribution tableDist;
        tableDist.hashValues["metric"] = {"0|65535", "40000"};
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashValuesSep["metric"] = "|";
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(3, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 2, 3));
    }
    { //pk with broadcast table
        TableDistribution tableDist;
        tableDist.partCnt = 1;
        tableDist.hashValues["metric"] = {"0", "65535"};
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(1, partIds.size());
        ASSERT_TRUE(*partIds.begin() >= 0 && *partIds.begin() <=3 );
    }

}

TEST_F(Ha3QrsSqlRequestGeneratorTest, getPartId) {
    {//hashId second less than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {1, 99};
        set<int32_t> partIds;
        Ha3QrsSqlRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(0, partIds.size());
    }
    {//hashId first less than ranges,hashId second in ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {1, 150};
        set<int32_t> partIds;
        Ha3QrsSqlRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    {//hashId first greater than ranges,hashId second less than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {100, 200};
        set<int32_t> partIds;
        Ha3QrsSqlRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    {//hashId first greater than ranges,hashId second less than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {120, 120};
        set<int32_t> partIds;
        Ha3QrsSqlRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    {//hashId first in ranges,hashId second greater than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {99, 300};
        set<int32_t> partIds;
        Ha3QrsSqlRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    {//hashId first greater than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {201, 300};
        set<int32_t> partIds;
        Ha3QrsSqlRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(0, partIds.size());
    }
    {//two ranges normal
        RangeVec ranges = {{100, 200}, {400, 600}};
        PartitionRange hashId = {200, 400};
        set<int32_t> partIds;
        Ha3QrsSqlRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1));
    }
    {//two ranges normal
        RangeVec ranges = {{100, 200}, {400, 600}};
        PartitionRange hashId = {201, 420};
        set<int32_t> partIds;
        Ha3QrsSqlRequestGenerator::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(1, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(1));
    }
}

TEST_F(Ha3QrsSqlRequestGeneratorTest, getPartIds) {
    {//no hashId query
        RangeVec ranges = {{100, 200}, {400, 600}};
        RangeVec hashIds = {{0, 99}, {300, 399}};
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::getPartIds(ranges, hashIds);
        ASSERT_EQ(0, partIds.size());
    }
    {//normal hashId
        RangeVec ranges = {{100, 200}, {400, 600}};
        RangeVec hashIds = {{0, 300}, {301, 620}};
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::getPartIds(ranges, hashIds);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1));
    }

}

TEST_F(Ha3QrsSqlRequestGeneratorTest, createHashFunc) {
    {
        HashMode hashMode;
        hashMode._hashFunction = "HASH";
        hashMode._hashFields.push_back("test");
        auto hashFunc = Ha3QrsSqlRequestGenerator::createHashFunc(hashMode);
        ASSERT_TRUE(hashFunc != nullptr);
    }
    {
        HashMode hashMode;
        hashMode._hashFunction = "HASH_not_exist";
        hashMode._hashFields.push_back("test");
        auto hashFunc = Ha3QrsSqlRequestGenerator::createHashFunc(hashMode);
        ASSERT_TRUE(hashFunc == nullptr);
    }
}

TEST_F(Ha3QrsSqlRequestGeneratorTest, genAllPart) {
    {
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAllPart(1);
        ASSERT_EQ(1, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    {
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAllPart(2);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0,1));
    }
    {
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAllPart(0);
        ASSERT_EQ(0, partIds.size());
    }
    {
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAllPart(-1);
        ASSERT_EQ(0, partIds.size());
    }
}

TEST_F(Ha3QrsSqlRequestGeneratorTest, genDebugAccessPart) {
    { //access all part
        TableDistribution tableDist;
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { //access part 3
        TableDistribution tableDist;
        tableDist.debugPartIds = "3";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(1, partIds.size());
        ASSERT_EQ(3, *partIds.begin());
    }
    { //access part out range
        TableDistribution tableDist;
        tableDist.debugPartIds = "4";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { //access part out range
        TableDistribution tableDist;
        tableDist.debugPartIds = "-1";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { //access part out range
        TableDistribution tableDist;
        tableDist.debugPartIds = "abc";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { //access part out range
        TableDistribution tableDist;
        tableDist.debugHashValues = "1,60000";
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 3));
    }
    { //access part out range
        TableDistribution tableDist;
        tableDist.debugHashValues = "1";
        tableDist.debugPartIds = "2,3";
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = Ha3QrsSqlRequestGenerator::genAccessPart(4, tableDist);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(2, 3));
    }
}

END_HA3_NAMESPACE();
