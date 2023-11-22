#include "sql/ops/tvf/builtin/TaobaoSpRerankTvfFunc.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "log/NaviLoggerProvider.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "sql/common/common.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace table;
using namespace testing;
using namespace matchdoc;

namespace sql {

class TaobaoSpRerankTvfFuncTest : public OpTestBase {
public:
    void setUp();
    void tearDown();

protected:
    TablePtr fakeTable();
    TablePtr fakeTable1();
    TablePtr fakeTable2();
    void testSuccess(const vector<string> &params,
                     TablePtr input,
                     const vector<int32_t> &expectIds,
                     KernelScope scope);
    void testSimpleSuccess(const vector<string> &params,
                           TablePtr input,
                           const vector<int32_t> &expectIds);
};

void TaobaoSpRerankTvfFuncTest::setUp() {}

void TaobaoSpRerankTvfFuncTest::tearDown() {}

TablePtr TaobaoSpRerankTvfFuncTest::fakeTable() {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr));
    const auto docs = allocator->batchAllocate(9);
    _matchDocUtil.extendMatchDocAllocator<int64_t>(
        allocator, docs, "trigger", {1, 1, 1, 1, 2, 2, 2, 3, 3});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "id", {100, 101, 102, 103, 200, 201, 202, 300, 301});
    _matchDocUtil.extendMatchDocAllocator(
        allocator, docs, "trigger_type", {"1", "1", "1", "1", "2", "2", "2", "3", "3"});
    _matchDocUtil.extendMatchDocAllocator(
        allocator, docs, "category", {"1", "2", "3", "4", "1", "1", "1", "2", "2"});
    // _matchDocUtil.extendMatchDocAllocator<float>(
    //     allocator, docs, "score", {1.1, 0.9, 0.7, 0.5, 1.3, 0.3, 0.2, 1.2, 0.4});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "score", {4, 3, 2, 1, 3, 2, 1, 2, 1});
    return Table::fromMatchDocs(docs, allocator);
}

TablePtr TaobaoSpRerankTvfFuncTest::fakeTable1() {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr));
    const auto docs = allocator->batchAllocate(9);
    _matchDocUtil.extendMatchDocAllocator<int64_t>(
        allocator, docs, "trigger", {1, 1, 1, 1, 1, 1, 1, 1, 1});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "id", {100, 101, 102, 103, 200, 201, 202, 300, 301});
    _matchDocUtil.extendMatchDocAllocator(
        allocator, docs, "trigger_type", {"1", "1", "1", "1", "1", "1", "1", "1", "1"});
    _matchDocUtil.extendMatchDocAllocator(
        allocator, docs, "category", {"1", "1", "1", "1", "2", "2", "2", "3", "3"});
    // _matchDocUtil.extendMatchDocAllocator<float>(
    //     allocator, docs, "score", {1.1, 0.9, 0.7, 0.5, 1.3, 0.3, 0.2, 1.2, 0.4});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "score", {4, 3, 2, 1, 3, 2, 1, 2, 1});
    return Table::fromMatchDocs(docs, allocator);
}

TablePtr TaobaoSpRerankTvfFuncTest::fakeTable2() {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr));
    const auto docs = allocator->batchAllocate(9);
    _matchDocUtil.extendMatchDocAllocator<int64_t>(
        allocator, docs, "trigger", {1, 1, 1, 1, 1, 1, 1, 1, 1});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "id", {100, 101, 102, 103, 200, 201, 202, 300, 301});
    _matchDocUtil.extendMultiValueMatchDocAllocator<std::string>(allocator,
                                                                 docs,
                                                                 "trigger_type",
                                                                 {{"1", "2"},
                                                                  {"1", "2"},
                                                                  {"1", "2"},
                                                                  {"1", "2"},
                                                                  {"1", "2"},
                                                                  {"1", "2"},
                                                                  {"1", "2"},
                                                                  {"1", "2"},
                                                                  {"1", "2"}});
    _matchDocUtil.extendMatchDocAllocator(
        allocator, docs, "category", {"1", "1", "1", "1", "2", "2", "2", "3", "3"});
    // _matchDocUtil.extendMatchDocAllocator<float>(
    //     allocator, docs, "score", {1.1, 0.9, 0.7, 0.5, 1.3, 0.3, 0.2, 1.2, 0.4});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "score", {4, 3, 2, 1, 3, 2, 1, 2, 1});
    return Table::fromMatchDocs(docs, allocator);
}

void TaobaoSpRerankTvfFuncTest::testSimpleSuccess(const vector<string> &params,
                                                  TablePtr input,
                                                  const vector<int32_t> &expectIds) {
    navi::NaviLoggerProvider provider("DEBUG");
    TvfFuncInitContext context;
    context.queryPool = _poolPtr.get();
    context.params = params;
    TaobaoSpRerankTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    checkOutputColumn(output, "id", expectIds);
}

TEST_F(TaobaoSpRerankTvfFuncTest, testInit) {
    {
        TvfFuncInitContext context;
        TaobaoSpRerankTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
}

TEST_F(TaobaoSpRerankTvfFuncTest, testSimpleMakeUp) {
    testSimpleSuccess({"trigger_type", "1;2;3", "5;1;1", "score"},
                      fakeTable(),
                      {103, 102, 101, 100, 202, 301, 300});
}

TEST_F(TaobaoSpRerankTvfFuncTest, testSimpleEmptyQuota) {
    testSimpleSuccess({"trigger_type", "1;2;3", "0;0;0", "score"}, fakeTable(), {});
}

TEST_F(TaobaoSpRerankTvfFuncTest, testComputeActualQuota) {
    navi::NaviLoggerProvider provider("DEBUG");
    { // test simple compute actual quota case 1
        TvfFuncInitContext context;
        context.queryPool = _poolPtr.get();
        context.params = vector<string>({"trigger_type", "3;2;1", "2500;1250;1250", "score"});
        TaobaoSpRerankTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_TRUE(tvfFunc._quotaNums[0] == 2500);
        std::vector<std::vector<table::Row>> groups;
        std::vector<size_t> actualQuota;
        groups.resize(3);
        groups[0].resize(3446);
        groups[1].resize(4858);
        groups[2].resize(239);
        actualQuota.resize(3);
        tvfFunc.computeActualQuota(groups, actualQuota);
        EXPECT_EQ(3174, actualQuota[0]);
        EXPECT_EQ(1587, actualQuota[1]);
        EXPECT_EQ(239, actualQuota[2]);
    }
}

TEST_F(TaobaoSpRerankTvfFuncTest, testComputeLeftCount) {
    navi::NaviLoggerProvider provider("DEBUG");
    { // test computeLeftCount succ
        TvfFuncInitContext context;
        context.queryPool = _poolPtr.get();
        context.params = vector<string>({"trigger_type", "3;2;1", "2500;1250;1250", "score"});
        TaobaoSpRerankTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_TRUE(tvfFunc._quotaNums[0] == 2500);
        std::vector<std::vector<table::Row>> groups;
        std::vector<size_t> actualQuota;
        groups.resize(3);
        groups[0].resize(3446);
        groups[1].resize(4858);
        groups[2].resize(239);
        actualQuota.resize(3);
        std::vector<size_t> groupsSize;
        for (const auto &group : groups) {
            groupsSize.emplace_back(group.size());
        }
        size_t leftCount = 5000;
        leftCount = tvfFunc.computeLeftCount(groups, leftCount, groupsSize, actualQuota);
        EXPECT_EQ(1011, leftCount);
        leftCount = tvfFunc.computeLeftCount(groups, leftCount, groupsSize, actualQuota);
        EXPECT_EQ(0, leftCount);
    }

    { // test computeLeftCount leftCount == 0
        TvfFuncInitContext context;
        context.queryPool = _poolPtr.get();
        context.params = vector<string>({"trigger_type", "3;2;1", "2500;1250;1250", "score"});
        TaobaoSpRerankTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_TRUE(tvfFunc._quotaNums[0] == 2500);
        std::vector<std::vector<table::Row>> groups;
        std::vector<size_t> actualQuota;
        groups.resize(3);
        groups[0].resize(3446);
        groups[1].resize(4858);
        groups[2].resize(239);
        actualQuota.resize(3);
        std::vector<size_t> groupsSize;
        for (const auto &group : groups) {
            groupsSize.emplace_back(group.size());
        }
        size_t leftCount = 0;
        leftCount = tvfFunc.computeLeftCount(groups, leftCount, groupsSize, actualQuota);
        EXPECT_EQ(0, leftCount);
    }

    { // test computeLeftCount leftCount == 0
        TvfFuncInitContext context;
        context.queryPool = _poolPtr.get();
        context.params = vector<string>({"trigger_type", "3;2;1", "2500;1250;1250", "score"});
        TaobaoSpRerankTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_TRUE(tvfFunc._quotaNums[0] == 2500);
        std::vector<std::vector<table::Row>> groups;
        std::vector<size_t> actualQuota;
        groups.resize(3);
        groups[0].resize(2500);
        groups[1].resize(1250);
        groups[2].resize(250);
        actualQuota.resize(3);
        std::vector<size_t> groupsSize;
        for (const auto &group : groups) {
            groupsSize.emplace_back(group.size());
        }
        size_t leftCount = 5000;
        leftCount = tvfFunc.computeLeftCount(groups, leftCount, groupsSize, actualQuota);
        EXPECT_EQ(1000, leftCount);
        leftCount = tvfFunc.computeLeftCount(groups, leftCount, groupsSize, actualQuota);
        EXPECT_EQ(0, leftCount);
    }
}

} // namespace sql
