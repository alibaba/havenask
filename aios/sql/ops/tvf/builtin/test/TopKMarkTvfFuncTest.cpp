#include "sql/ops/tvf/builtin/TopKMarkTvfFunc.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "iquan/common/catalog/TvfFunctionModel.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/log/NaviLoggerProvider.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace table;
using namespace matchdoc;

namespace sql {

class TopKMarkTvfFuncTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
    TablePtr prepareInputTable(vector<int32_t> value);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, TopKMarkTvfFuncTest);

void TopKMarkTvfFuncTest::setUp() {}

void TopKMarkTvfFuncTest::tearDown() {}

TablePtr TopKMarkTvfFuncTest::prepareInputTable(vector<int32_t> value) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(value);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", value);
    TablePtr table;
    table.reset(new Table(docs, allocator));
    return table;
}

TEST_F(TopKMarkTvfFuncTest, testInit) {
    { // param expect 2
        TvfFuncInitContext context;
        TopKMarkTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    { // param expect 2
        TvfFuncInitContext context;
        context.params = {"abc", "123", "ab"};
        TopKMarkTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    { // sort field empty
        TvfFuncInitContext context;
        context.params = {"", "1"};
        TopKMarkTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params = {"123", "-1"};
        TopKMarkTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(std::numeric_limits<int32_t>::max(), tvfFunc._count);
        vector<string> expectSVec = {"123"};
        vector<bool> fVec = {false};
        ASSERT_EQ(expectSVec, tvfFunc._sortFields);
        ASSERT_EQ(fVec, tvfFunc._sortFlags);
    }
    {
        TvfFuncInitContext context;
        context.params = {"123", "1"};
        TopKMarkTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(1, tvfFunc._count);
        vector<string> expectSVec = {"123"};
        vector<bool> fVec = {false};
        ASSERT_EQ(expectSVec, tvfFunc._sortFields);
        ASSERT_EQ(fVec, tvfFunc._sortFlags);
    }
    {
        TvfFuncInitContext context;
        context.params = {"123,456", "1"};
        TopKMarkTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(1, tvfFunc._count);
        vector<string> expectSVec = {"123", "456"};
        vector<bool> fVec = {false, false};
        ASSERT_EQ(expectSVec, tvfFunc._sortFields);
        ASSERT_EQ(fVec, tvfFunc._sortFlags);
    }
    {
        TvfFuncInitContext context;
        context.params = {"+123,-456,", "1"};
        TopKMarkTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(1, tvfFunc._count);
        vector<string> expectSVec = {"123", "456"};
        vector<bool> fVec = {false, true};
        ASSERT_EQ(expectSVec, tvfFunc._sortFields);
        ASSERT_EQ(fVec, tvfFunc._sortFlags);
    }
}

TEST_F(TopKMarkTvfFuncTest, testdoComputeCreateComparatorFail) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(3);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {3, 2, 1});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {3, 3, 2});
    TablePtr input(new Table(docs, allocator));
    TvfFuncInitContext context;
    context.params = {"value1", "2"};
    context.queryPool = _poolPtr.get();
    TopKMarkTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    ASSERT_FALSE(tvfFunc.doCompute(input));
}

TEST_F(TopKMarkTvfFuncTest, testdoComputeDefaultSort) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(5);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {5, 3, 2, 1, 4});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {6, 7, 8, 10, 9});
    TablePtr input(new Table(docs, allocator));
    TvfFuncInitContext context;
    context.params = {"value", "2"};
    context.queryPool = _poolPtr.get();
    TopKMarkTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(5, output->getRowCount());
    vector<int32_t> expectId = {5, 3, 2, 1, 4};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {6, 7, 8, 10, 9};
    checkOutputColumn<int32_t>(output, "value", expectValue);
    vector<int32_t> expectTopKMark = {1, 1, 0, 0, 0};
    checkOutputColumn<int32_t>(output, "topKMark", expectTopKMark);
}

TEST_F(TopKMarkTvfFuncTest, testdoComputeDefaultSortTopKMark) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(5);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {5, 3, 2, 1, 4});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {7, 6, 8, 10, 9});
    TablePtr input(new Table(docs, allocator));
    TvfFuncInitContext context;
    context.params = {"value", "4"};
    context.queryPool = _poolPtr.get();
    TopKMarkTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(5, output->getRowCount());
    vector<int32_t> expectId = {2, 3, 5, 4, 1};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {8, 6, 7, 9, 10};
    checkOutputColumn<int32_t>(output, "value", expectValue);
    vector<int32_t> expectTopKMark = {1, 1, 1, 1, 0};
    checkOutputColumn<int32_t>(output, "topKMark", expectTopKMark);
}

TEST_F(TopKMarkTvfFuncTest, testComputeDefaultSortDesc) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(5);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {5, 3, 2, 1, 4});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {6, 7, 8, 10, 9});
    TablePtr input(new Table(docs, allocator));
    TvfFuncInitContext context;
    context.params = {"-value", "2"};
    context.queryPool = _poolPtr.get();
    TopKMarkTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(5, output->getRowCount());
    vector<int32_t> expectId = {1, 4, 2, 5, 3};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {10, 9, 8, 6, 7};
    checkOutputColumn<int32_t>(output, "value", expectValue);
    vector<int32_t> expectTopKMark = {1, 1, 0, 0, 0};
    checkOutputColumn<int32_t>(output, "topKMark", expectTopKMark);
}

TEST_F(TopKMarkTvfFuncTest, testComputeInputEof) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(5);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {5, 3, 2, 1, 4});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {6, 7, 8, 10, 9});
    TablePtr input(new Table(docs, allocator));
    TvfFuncInitContext context;
    context.params = {"value", "2"};
    context.queryPool = _poolPtr.get();
    TopKMarkTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.compute(input, false, output));
    ASSERT_TRUE(output == nullptr);
    input.reset();
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(5, output->getRowCount());
    vector<int32_t> expectId = {5, 3, 2, 1, 4};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {6, 7, 8, 10, 9};
    checkOutputColumn<int32_t>(output, "value", expectValue);
    vector<int32_t> expectTopKMark = {1, 1, 0, 0, 0};
    checkOutputColumn<int32_t>(output, "topKMark", expectTopKMark);
}

TEST_F(TopKMarkTvfFuncTest, testComputeTopKMark_0) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(5);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {5, 3, 2, 1, 4});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {6, 7, 8, 10, 9});
    TablePtr input(new Table(docs, allocator));
    TvfFuncInitContext context;
    context.params = {"value", "0"};
    context.queryPool = _poolPtr.get();
    TopKMarkTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(5, output->getRowCount());
    vector<int32_t> expectId = {5, 3, 2, 1, 4};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {6, 7, 8, 10, 9};
    checkOutputColumn<int32_t>(output, "value", expectValue);
    vector<int32_t> expectTopKMark = {0, 0, 0, 0, 0};
    checkOutputColumn<int32_t>(output, "topKMark", expectTopKMark);
}

TEST_F(TopKMarkTvfFuncTest, testComputeLessThanTopKMark) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(3);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {3, 2, 1});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {3, 2, 1});
    TablePtr input(new Table(docs, allocator));
    TvfFuncInitContext context;
    context.params = {"-value", "4"};
    context.queryPool = _poolPtr.get();
    TopKMarkTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(3, output->getRowCount());
    vector<int32_t> expectId = {3, 2, 1};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {3, 2, 1};
    checkOutputColumn<int32_t>(output, "value", expectValue);
    vector<int32_t> expectTopKMark = {1, 1, 1};
    checkOutputColumn<int32_t>(output, "topKMark", expectTopKMark);
}

TEST_F(TopKMarkTvfFuncTest, testComputeLessThanTopKMark_max) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(3);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {3, 2, 1});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {3, 2, 1});
    TablePtr input(new Table(docs, allocator));
    TvfFuncInitContext context;
    context.params = {"-value", "-1"};
    context.queryPool = _poolPtr.get();
    TopKMarkTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(3, output->getRowCount());
    vector<int32_t> expectId = {3, 2, 1};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {3, 2, 1};
    checkOutputColumn<int32_t>(output, "value", expectValue);
    vector<int32_t> expectTopKMark = {1, 1, 1};
    checkOutputColumn<int32_t>(output, "topKMark", expectTopKMark);
}

TEST_F(TopKMarkTvfFuncTest, testComputeEmptyInput) {
    TablePtr input;
    TvfFuncInitContext context;
    context.params = {"-value", "-1"};
    context.queryPool = _poolPtr.get();
    TopKMarkTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output == nullptr);
}

TEST_F(TopKMarkTvfFuncTest, testRegTvfModels) {
    navi::NaviLoggerProvider provider("DEBUG");
    TopKMarkTvfFuncCreator creator;
    iquan::TvfModels tvfModels;
    ASSERT_TRUE(creator.regTvfModels(tvfModels));
    ASSERT_EQ(1, tvfModels.functions.size());
}

} // namespace sql
