#include "sql/ops/join/HashJoinMapR.h"

#include "autil/HashUtil.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;

namespace sql {

class HashJoinMapRTest : public TESTBASE {
public:
    HashJoinMapRTest();
    ~HashJoinMapRTest();

private:
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    table::MatchDocUtil _matchDocUtil;

private:
    void checkHashValuesOffset(const HashJoinMapR::HashValues &hashValues,
                               HashJoinMapR::HashValues expectValues) {
        ASSERT_EQ(hashValues.size(), expectValues.size());
        for (size_t i = 0; i < hashValues.size(); i++) {
            EXPECT_EQ(expectValues[i], hashValues[i]) << i;
        }
    }
    void checkHashValuesOffsetWithHash(const HashJoinMapR::HashValues &hashValues,
                                       HashJoinMapR::HashValues expectValues) {
        ASSERT_EQ(hashValues.size(), expectValues.size());
        for (size_t i = 0; i < expectValues.size(); i++) {
            auto hashVal = expectValues[i];
            hashVal.second = autil::HashUtil::calculateHashValue<int32_t>(hashVal.second);
            ASSERT_EQ(hashVal, hashValues[i]) << i;
        }
    }
};

HashJoinMapRTest::HashJoinMapRTest()
    : _poolPtr(new autil::mem_pool::PoolAsan())
    , _matchDocUtil(_poolPtr) {}

HashJoinMapRTest::~HashJoinMapRTest() {}

TEST_F(HashJoinMapRTest, testCombineHashValues) {
    {
        HashJoinMapR::HashValues left = {{1, 10}, {2, 22}, {3, 33}};
        HashJoinMapR::HashValues right = {{1, 100}, {2, 122}, {3, 133}};
        HashJoinMapR hashJoinMapR;
        hashJoinMapR.combineHashValues(left, right);
        ASSERT_EQ(3, right.size());
    }
    {
        HashJoinMapR::HashValues left = {};
        HashJoinMapR::HashValues right = {{1, 100}, {2, 122}, {3, 133}};
        HashJoinMapR hashJoinMapR;
        hashJoinMapR.combineHashValues(left, right);
        ASSERT_EQ(0, right.size());
    }
    {
        HashJoinMapR::HashValues left = {{1, 10}, {2, 22}, {3, 33}};
        HashJoinMapR::HashValues right = {};
        HashJoinMapR hashJoinMapR;
        hashJoinMapR.combineHashValues(left, right);
        ASSERT_EQ(0, right.size());
    }
    {
        HashJoinMapR::HashValues left = {{1, 10}, {3, 22}, {5, 33}};
        HashJoinMapR::HashValues right = {{2, 100}, {4, 122}, {6, 133}};
        HashJoinMapR hashJoinMapR;
        hashJoinMapR.combineHashValues(left, right);
        ASSERT_EQ(0, right.size());
    }
    {
        HashJoinMapR::HashValues left = {{1, 10}, {1, 22}, {1, 33}};
        HashJoinMapR::HashValues right = {{1, 100}, {1, 122}, {3, 133}};
        HashJoinMapR hashJoinMapR;
        hashJoinMapR.combineHashValues(left, right);
        ASSERT_EQ(6, right.size());
    }
    {
        HashJoinMapR::HashValues left = {{2, 10}};
        HashJoinMapR::HashValues right = {{1, 100}, {2, 22}, {3, 33}};
        HashJoinMapR hashJoinMapR;
        hashJoinMapR.combineHashValues(left, right);
        ASSERT_EQ(1, right.size());
    }
}

TEST_F(HashJoinMapRTest, testGetColumnHashValues) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 3);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid2", {2, 1, 0}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
        allocator, leftDocs, "cid", {"1111", "3333", "1111"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1, 3}, {2}, {4}}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "mcid", {{1, 3, 4}, {2, 4, 5, 6}, {}}));
    TablePtr inputTable = Table::fromMatchDocs(leftDocs, allocator);
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getColumnHashValues(
            inputTable, 0, inputTable->getRowCount(), "uid", hashValues));
        ASSERT_FALSE(hashJoinMapR._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(
            checkHashValuesOffsetWithHash(hashValues, {{0, 0}, {1, 1}, {2, 2}}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getColumnHashValues(inputTable, 0, 1, "uid", hashValues));
        ASSERT_FALSE(hashJoinMapR._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(hashValues, {{0, 0}}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getColumnHashValues(inputTable, 1, 2, "uid", hashValues));
        ASSERT_FALSE(hashJoinMapR._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(hashValues, {{1, 1}, {2, 2}}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getColumnHashValues(inputTable, 2, 4, "uid", hashValues));
        ASSERT_FALSE(hashJoinMapR._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(hashValues, {{2, 2}}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getColumnHashValues(inputTable, 5, 4, "uid", hashValues));
        ASSERT_TRUE(hashJoinMapR._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(hashValues, {}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getColumnHashValues(inputTable, 0, 4, "mcid", hashValues));
        ASSERT_FALSE(hashJoinMapR._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(
            hashValues, {{0, 1}, {0, 3}, {0, 4}, {1, 2}, {1, 4}, {1, 5}, {1, 6}}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getColumnHashValues(inputTable, 2, 1, "mcid", hashValues));
        ASSERT_TRUE(hashJoinMapR._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(hashValues, {}));
    }
}

TEST_F(HashJoinMapRTest, testGetHashValues) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 3);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid2", {2, 1, 0}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
        allocator, leftDocs, "cid", {"1111", "3333", "1111"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1, 3}, {2}, {4}}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "mcid", {{1, 3, 4}, {2, 4, 5, 6}, {}}));
    TablePtr inputTable = Table::fromMatchDocs(leftDocs, allocator);
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getHashValues(inputTable, 0, 3, {"uid"}, hashValues));
        ASSERT_NO_FATAL_FAILURE(
            checkHashValuesOffsetWithHash(hashValues, {{0, 0}, {1, 1}, {2, 2}}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getHashValues(inputTable, 0, 3, {"uid2"}, hashValues));
        ASSERT_NO_FATAL_FAILURE(
            checkHashValuesOffsetWithHash(hashValues, {{0, 2}, {1, 1}, {2, 0}}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getHashValues(inputTable, 0, 3, {"uid", "uid2"}, hashValues));
        size_t v1 = autil::HashUtil::calculateHashValue(2);
        autil::HashUtil::combineHash(v1, 0);
        size_t v2 = autil::HashUtil::calculateHashValue(1);
        autil::HashUtil::combineHash(v2, 1);
        size_t v3 = autil::HashUtil::calculateHashValue(0);
        autil::HashUtil::combineHash(v3, 2);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffset(hashValues, {{0, v1}, {1, v2}, {2, v3}}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getHashValues(inputTable, 0, 3, {"cid"}, hashValues));
        ASSERT_NO_FATAL_FAILURE(
            checkHashValuesOffset(hashValues,
                                  {{0, autil::HashUtil::calculateHashValue(string("1111"))},
                                   {1, autil::HashUtil::calculateHashValue(string("3333"))},
                                   {2, autil::HashUtil::calculateHashValue(string("1111"))}}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getHashValues(inputTable, 0, 3, {"mcid"}, hashValues));
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(
            hashValues, {{0, 1}, {0, 3}, {0, 4}, {1, 2}, {1, 4}, {1, 5}, {1, 6}}));
    }
    {
        HashJoinMapR hashJoinMapR;
        HashJoinMapR::HashValues hashValues;
        ASSERT_TRUE(hashJoinMapR.getHashValues(inputTable, 0, 3, {"muid", "mcid"}, hashValues));
        size_t v1 = autil::HashUtil::calculateHashValue(1);
        autil::HashUtil::combineHash(v1, 1);
        size_t v2 = autil::HashUtil::calculateHashValue(1);
        autil::HashUtil::combineHash(v2, 3);
        size_t v3 = autil::HashUtil::calculateHashValue(3);
        autil::HashUtil::combineHash(v3, 1);
        size_t v4 = autil::HashUtil::calculateHashValue(3);
        autil::HashUtil::combineHash(v4, 3);
        size_t v5 = autil::HashUtil::calculateHashValue(4);
        autil::HashUtil::combineHash(v5, 1);
        size_t v6 = autil::HashUtil::calculateHashValue(4);
        autil::HashUtil::combineHash(v6, 3);
        size_t v7 = autil::HashUtil::calculateHashValue(2);
        autil::HashUtil::combineHash(v7, 2);
        size_t v8 = autil::HashUtil::calculateHashValue(4);
        autil::HashUtil::combineHash(v8, 2);
        size_t v9 = autil::HashUtil::calculateHashValue(5);
        autil::HashUtil::combineHash(v9, 2);
        size_t v10 = autil::HashUtil::calculateHashValue(6);
        autil::HashUtil::combineHash(v10, 2);

        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffset(hashValues,
                                                      {{0, v1},
                                                       {0, v2},
                                                       {0, v3},
                                                       {0, v4},
                                                       {0, v5},
                                                       {0, v6},
                                                       {1, v7},
                                                       {1, v8},
                                                       {1, v9},
                                                       {1, v10}}));
    }
}

} // namespace sql
