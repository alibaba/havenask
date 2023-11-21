#include "sql/ops/tvf/builtin/UnPackMultiValueTvfFunc.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/VectorDocStorage.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace table;
using namespace matchdoc;
using namespace autil;

namespace sql {

class UnPackMultiValueTvfFuncTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
};

void UnPackMultiValueTvfFuncTest::setUp() {}

void UnPackMultiValueTvfFuncTest::tearDown() {}
TEST_F(UnPackMultiValueTvfFuncTest, testComputeInputTableNull) {
    // empty input
    UnPackMultiValueTvfFunc func;
    TablePtr output;
    ASSERT_TRUE(func.compute(TablePtr(), true, output));
    ASSERT_TRUE(func.compute(TablePtr(), false, output));
}
TEST_F(UnPackMultiValueTvfFuncTest, testComputeInputTableRowCount0) {
    // input size = 0
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 0);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(allocator, leftDocs, "muid", {}));
    TablePtr input = Table::fromMatchDocs(leftDocs, allocator);
    UnPackMultiValueTvfFunc func;
    func._unpackFields.push_back("muid");
    TablePtr output;
    ASSERT_TRUE(func.compute(input, true, output));
    ASSERT_EQ(input.get(), output.get());
}
TEST_F(UnPackMultiValueTvfFuncTest, testComputeUnpackFieldEmpty) {
    // unpack field empty
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 1);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1}}));
    TablePtr input = Table::fromMatchDocs(leftDocs, allocator);
    UnPackMultiValueTvfFunc func;
    TablePtr output;
    ASSERT_TRUE(func.compute(input, true, output));
    ASSERT_EQ(input.get(), output.get());
}
TEST_F(UnPackMultiValueTvfFuncTest, testComputeUnpackFieldNotExist) {
    // unpack field not equal
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 1);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1, 2, 3}}));
    TablePtr input = Table::fromMatchDocs(leftDocs, allocator);
    UnPackMultiValueTvfFunc func;
    func._unpackFields.push_back("muid1");
    TablePtr output;
    ASSERT_TRUE(func.compute(input, true, output));
    ASSERT_EQ(input.get(), output.get());
    vector<vector<int32_t>> expect = {{1, 2, 3}};
    checkOutputMultiColumn<int32_t>(output, "muid", expect);
}

TEST_F(UnPackMultiValueTvfFuncTest, testComputeUnpackOneField) {
    // unpack one field
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 1);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, leftDocs, "uid", {1}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1, 2, 3}}));
    TablePtr input = Table::fromMatchDocs(leftDocs, allocator);
    UnPackMultiValueTvfFunc func;
    func._unpackFields.push_back("muid");
    TablePtr output;
    ASSERT_TRUE(func.compute(input, true, output));
    ASSERT_EQ(input.get(), output.get());
    ASSERT_EQ(3, output->getRowCount());
    vector<int32_t> expectUid = {1, 1, 1};
    checkOutputColumn<int32_t>(output, "uid", expectUid);

    vector<vector<int32_t>> expectMuid = {{1}, {2}, {3}};
    checkOutputMultiColumn<int32_t>(output, "muid", expectMuid);
}

TEST_F(UnPackMultiValueTvfFuncTest, testComputeUnpackMultiField) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 6);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, leftDocs, "uid", {0, 1, 2, 3, 4, 5}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1}, {}, {}, {2, 4}, {2, 3}, {5}}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
        allocator, leftDocs, "msuid", {{}, {"1"}, {"2", "4"}, {"2", "3"}, {}, {"5"}}));
    TablePtr input = Table::fromMatchDocs(leftDocs, allocator);
    UnPackMultiValueTvfFunc func;
    func._unpackFields.push_back("uid");
    func._unpackFields.push_back("muid");
    func._unpackFields.push_back("msuid");
    TablePtr output;
    ASSERT_TRUE(func.compute(input, true, output));
    ASSERT_EQ(input.get(), output.get());
    ASSERT_EQ(11, output->getRowCount());
    vector<int32_t> expectUid = {0, 1, 2, 3, 4, 5, 4, 3, 2, 3, 3};
    checkOutputColumn<int32_t>(output, "uid", expectUid);
    vector<vector<int32_t>> expectMuid = {{1}, {}, {}, {2}, {2}, {5}, {3}, {4}, {}, {2}, {4}};
    checkOutputMultiColumn<int32_t>(output, "muid", expectMuid);
    vector<vector<string>> expectMsuid
        = {{}, {"1"}, {"2"}, {"2"}, {}, {"5"}, {}, {"2"}, {"4"}, {"3"}, {"3"}};
    checkOutputMultiColumn<string>(output, "msuid", expectMsuid);
}

TEST_F(UnPackMultiValueTvfFuncTest, testMultiValue) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 5);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, leftDocs, "uid", {0, 1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1}, {}, {2, 4}, {1, 2, 3}, {}}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
        allocator, leftDocs, "msuid", {{"1"}, {}, {"2", "4"}, {"1", "2", "3"}, {}}));
    TablePtr table = Table::fromMatchDocs(leftDocs, allocator);
    UnPackMultiValueTvfFunc func;
    auto col = table->getColumn("muid");
    ASSERT_TRUE(col != nullptr);

    ASSERT_TRUE(func.unpackMultiValue(col, table));
    vector<vector<int32_t>> expect = {{1}, {}, {2}, {1}, {}, {2}, {3}, {4}};
    checkOutputMultiColumn<int32_t>(table, "muid", expect);

    vector<int32_t> expectUid = {0, 1, 2, 3, 4, 3, 3, 2};
    checkOutputColumn<int32_t>(table, "uid", expectUid);

    vector<vector<string>> expectMsuid = {
        {"1"}, {}, {"2", "4"}, {"1", "2", "3"}, {}, {"1", "2", "3"}, {"1", "2", "3"}, {"2", "4"}};
    checkOutputMultiColumn<string>(table, "msuid", expectMsuid);
}

TEST_F(UnPackMultiValueTvfFuncTest, testUnpackTable) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 5);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, leftDocs, "uid", {0, 1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1}, {}, {2, 4}, {1, 2, 3}, {}}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
        allocator, leftDocs, "msuid", {{"1"}, {}, {"2", "4"}, {"1", "2", "3"}, {}}));
    TablePtr table = Table::fromMatchDocs(leftDocs, allocator);
    UnPackMultiValueTvfFunc func;
    vector<pair<int32_t, int32_t>> unpackVec
        = {{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 0}, {3, 1}, {3, 2}, {2, 1}};

    ASSERT_TRUE(func.unpackTable(unpackVec, "muid", table));
    vector<vector<int32_t>> expect = {{1}, {}, {2}, {1}, {}, {2}, {3}, {4}};
    checkOutputMultiColumn<int32_t>(table, "muid", expect);

    vector<int32_t> expectUid = {0, 1, 2, 3, 4, 3, 3, 2};
    checkOutputColumn<int32_t>(table, "uid", expectUid);

    vector<vector<string>> expectMsuid = {
        {"1"}, {}, {"2", "4"}, {"1", "2", "3"}, {}, {"1", "2", "3"}, {"1", "2", "3"}, {"2", "4"}};
    checkOutputMultiColumn<string>(table, "msuid", expectMsuid);
}

TEST_F(UnPackMultiValueTvfFuncTest, testCopyUnpackCol) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 5);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, leftDocs, "uid", {0, 1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1}, {}, {2, 4}, {1, 2, 3}, {}}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
        allocator, leftDocs, "msuid", {{"1"}, {}, {"2", "4"}, {"1", "2", "3"}, {}}));
    TablePtr table = Table::fromMatchDocs(leftDocs, allocator);
    UnPackMultiValueTvfFunc func;
    vector<pair<int32_t, int32_t>> unpackVec
        = {{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 0}, {3, 1}, {3, 2}, {2, 1}};
    table->batchAllocateRow(unpackVec.size() - 5);
    {
        auto col = table->getColumn("muid");
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(func.copyUnpackCol(5, table->getRows(), unpackVec, col));
        vector<vector<int32_t>> expect = {{1}, {}, {2}, {1}, {}, {2}, {3}, {4}};
        checkOutputMultiColumn<int32_t>(table, "muid", expect);
    }
    {
        auto col = table->getColumn("msuid");
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(func.copyUnpackCol(5, table->getRows(), unpackVec, col));
        vector<vector<string>> expect = {{"1"}, {}, {"2"}, {"1"}, {}, {"2"}, {"3"}, {"4"}};
        checkOutputMultiColumn<string>(table, "msuid", expect);
    }
    {
        auto col = table->getColumn("uid");
        ASSERT_TRUE(col != nullptr);
        ASSERT_FALSE(func.copyUnpackCol(5, table->getRows(), unpackVec, col));
    }
}

TEST_F(UnPackMultiValueTvfFuncTest, testCopyNormalCol) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 5);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, leftDocs, "uid", {0, 1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1}, {}, {2, 4}, {1, 2, 3}, {}}));
    TablePtr table = Table::fromMatchDocs(leftDocs, allocator);
    UnPackMultiValueTvfFunc func;
    vector<pair<int32_t, int32_t>> unpackVec
        = {{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 0}, {3, 1}, {3, 2}, {2, 1}};
    table->batchAllocateRow(unpackVec.size() - 5);
    {
        auto col = table->getColumn("muid");
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(func.copyNormalCol(5, table->getRows(), unpackVec, col));
        vector<vector<int32_t>> expect
            = {{1}, {}, {2, 4}, {1, 2, 3}, {}, {1, 2, 3}, {1, 2, 3}, {2, 4}};
        checkOutputMultiColumn<int32_t>(table, "muid", expect);
    }
    {
        auto col = table->getColumn("uid");
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(func.copyNormalCol(5, table->getRows(), unpackVec, col));
        vector<int32_t> expect = {0, 1, 2, 3, 4, 3, 3, 2};
        checkOutputColumn<int32_t>(table, "uid", expect);
    }
}

TEST_F(UnPackMultiValueTvfFuncTest, testGenColumnUnpackOffsetVec) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 5);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
        allocator, leftDocs, "uid", {0, 1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1}, {}, {2, 4}, {1, 2, 3}, {}}));
    TablePtr table = Table::fromMatchDocs(leftDocs, allocator);
    { // not multi col
        auto col = table->getColumn("uid");
        ASSERT_TRUE(col != nullptr);
        UnPackMultiValueTvfFunc func;
        vector<pair<int32_t, int32_t>> unpackVec;
        ASSERT_FALSE(func.genColumnUnpackOffsetVec(col, unpackVec));
    }
    { // multi col
        auto col = table->getColumn("muid");
        ASSERT_TRUE(col != nullptr);
        UnPackMultiValueTvfFunc func;
        vector<pair<int32_t, int32_t>> unpackVec;
        ASSERT_TRUE(func.genColumnUnpackOffsetVec(col, unpackVec));
        ASSERT_EQ(8, unpackVec.size());
        vector<pair<int32_t, int32_t>> expect
            = {{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 0}, {3, 1}, {3, 2}, {2, 1}};
        ASSERT_EQ(expect, unpackVec);
    }
}

TEST_F(UnPackMultiValueTvfFuncTest, testRegTvfModels) {
    UnPackMultiValueTvfFuncCreator creator;
    std::vector<iquan::FunctionModel> tvfModels;
    ASSERT_TRUE(creator.regTvfModels(tvfModels));
    ASSERT_EQ(1, tvfModels.size());
}

} // namespace sql
