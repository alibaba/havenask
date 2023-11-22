#include "sql/ops/tvf/builtin/RankTvfFunc.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "iquan/common/catalog/FunctionModel.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
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

class RankTvfFuncTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
    TablePtr prepareInputTable(vector<int32_t> value);
};

void RankTvfFuncTest::setUp() {}

void RankTvfFuncTest::tearDown() {}

TablePtr RankTvfFuncTest::prepareInputTable(vector<int32_t> value) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(value);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", value);
    return Table::fromMatchDocs(docs, allocator);
}

TEST_F(RankTvfFuncTest, testInit) {
    {
        TvfFuncInitContext context;
        RankTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params = {"abc", "123", "ab"};
        RankTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params = {"abc", "", "ab"};
        RankTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params = {"abc", "123", "1"};
        RankTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(1, tvfFunc._count);
        vector<string> expectPVec = {"abc"};
        vector<string> expectSVec = {"123"};
        vector<bool> fVec = {false};
        ASSERT_EQ(expectPVec, tvfFunc._partitionFields);
        ASSERT_EQ(expectSVec, tvfFunc._sortFields);
        ASSERT_EQ(fVec, tvfFunc._sortFlags);
    }
    {
        TvfFuncInitContext context;
        context.params = {"", "123,456", "1"};
        RankTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(1, tvfFunc._count);
        vector<string> expectPVec = {};
        vector<string> expectSVec = {"123", "456"};
        vector<bool> fVec = {false, false};
        ASSERT_EQ(expectPVec, tvfFunc._partitionFields);
        ASSERT_EQ(expectSVec, tvfFunc._sortFields);
        ASSERT_EQ(fVec, tvfFunc._sortFlags);
    }
    {
        TvfFuncInitContext context;
        context.params = {"a,b", "+123,-456,", "1"};
        RankTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(1, tvfFunc._count);
        vector<string> expectPVec = {"a", "b"};
        vector<string> expectSVec = {"123", "456"};
        vector<bool> fVec = {false, true};
        ASSERT_EQ(expectPVec, tvfFunc._partitionFields);
        ASSERT_EQ(expectSVec, tvfFunc._sortFields);
        ASSERT_EQ(fVec, tvfFunc._sortFlags);
    }
}

TEST_F(RankTvfFuncTest, testdoComputeCreateComparatorFail) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(3);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {3, 2, 1});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {3, 3, 2});
    TablePtr input = Table::fromMatchDocs(docs, allocator);
    TvfFuncInitContext context;
    context.params = {"id", "value1", "2"};
    context.queryPool = _poolPtr.get();
    RankTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_FALSE(tvfFunc.doCompute(input, output));
}

TEST_F(RankTvfFuncTest, testdoComputePartitionKeyFailed) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(2);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {3, 2});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {3, 2});
    TablePtr input = Table::fromMatchDocs(docs, allocator);
    TvfFuncInitContext context;
    context.params = {"id1", "value", "1"};
    context.queryPool = _poolPtr.get();
    RankTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_FALSE(tvfFunc.doCompute(input, output));
}

TEST_F(RankTvfFuncTest, testdoComputeSimpleLessThanWithError) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(1);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {3});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {3});
    TablePtr input = Table::fromMatchDocs(docs, allocator);
    TvfFuncInitContext context;
    context.params = {"id1", "value1", "2"};
    context.queryPool = _poolPtr.get();
    RankTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.doCompute(input, output));
    vector<int32_t> expectId = {3};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {3};
    checkOutputColumn<int32_t>(output, "value", expectValue);
}

TEST_F(RankTvfFuncTest, testdoComputeDefaultSort) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(11);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "id", {3, 2, 1, 4, 3, 3, 2, 4, 4, 4, 5});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "value", {3, 2, 1, 4, 1, 2, 1, 1, 2, 3, 5});
    TablePtr input = Table::fromMatchDocs(docs, allocator);
    TvfFuncInitContext context;
    context.params = {"id", "value", "2"};
    context.queryPool = _poolPtr.get();
    RankTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.doCompute(input, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(8, output->getRowCount());
    vector<int32_t> expectId = {2, 1, 3, 3, 2, 4, 4, 5};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {2, 1, 1, 2, 1, 1, 2, 5};
    checkOutputColumn<int32_t>(output, "value", expectValue);
}

TEST_F(RankTvfFuncTest, testdoComputeDefaultSortDesc) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(10);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "id", {3, 2, 1, 4, 3, 3, 2, 4, 4, 4});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "value", {3, 2, 1, 4, 1, 2, 1, 1, 2, 3});
    TablePtr input = Table::fromMatchDocs(docs, allocator);
    TvfFuncInitContext context;
    context.params = {"id", "-value", "2"};
    context.queryPool = _poolPtr.get();
    RankTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.doCompute(input, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(7, output->getRowCount());
    vector<int32_t> expectId = {3, 2, 1, 4, 3, 2, 4};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {3, 2, 1, 4, 2, 1, 3};
    checkOutputColumn<int32_t>(output, "value", expectValue);
}

TEST_F(RankTvfFuncTest, testdoComputePartitionFieldEmpty) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(11);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "id", {3, 2, 1, 4, 3, 3, 2, 4, 4, 4, 5});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(
        allocator, docs, "value", {3, 2, 1, 4, 1, 2, 1, 1, 2, 3, 5});
    TablePtr input = Table::fromMatchDocs(docs, allocator);
    TvfFuncInitContext context;
    context.params = {"", "value", "5"};
    context.queryPool = _poolPtr.get();
    RankTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.doCompute(input, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(5, output->getRowCount());
    vector<int32_t> expectId = {2, 1, 3, 2, 4};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {2, 1, 1, 1, 1};
    checkOutputColumn<int32_t>(output, "value", expectValue);
}

TEST_F(RankTvfFuncTest, testdoComputeLessThanTopK) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(3);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", {3, 2, 1});
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "value", {3, 2, 1});
    TablePtr input = Table::fromMatchDocs(docs, allocator);
    TvfFuncInitContext context;
    context.params = {"id", "-value", "3"};
    context.queryPool = _poolPtr.get();
    RankTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.doCompute(input, output));
    ASSERT_TRUE(output != nullptr);
    ASSERT_EQ(3, output->getRowCount());
    vector<int32_t> expectId = {3, 2, 1};
    checkOutputColumn<int32_t>(output, "id", expectId);
    vector<int32_t> expectValue = {3, 2, 1};
    checkOutputColumn<int32_t>(output, "value", expectValue);
}

TEST_F(RankTvfFuncTest, testRegTvfModels) {
    RankTvfFuncCreator creator;
    std::vector<iquan::FunctionModel> tvfModels;
    ASSERT_TRUE(creator.regTvfModels(tvfModels));
    ASSERT_EQ(1, tvfModels.size());
}

} // namespace sql
