#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/tvf/builtin/DistinctTopNTvfFunc.h"
#include <ha3/sql/data/TableUtil.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class DistinctTopNTvfFuncTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
protected:
    TablePtr fakeTable1();
    void testSuccess(
        const vector<string>& params,
        TablePtr input,
        const vector<int32_t>& expectIds,
        KernelScope scope);
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(builtin, DistinctTopNTvfFuncTest);

void DistinctTopNTvfFuncTest::setUp() {
}

void DistinctTopNTvfFuncTest::tearDown() {
}

TablePtr DistinctTopNTvfFuncTest::fakeTable1() {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr));
    const auto docs = allocator->batchAllocate(9);
    extendMatchDocAllocator<int64_t>(
        allocator, docs, "trigger",
        {111, 111, 111, 111, 222, 222, 222, 111, 111});
    extendMatchDocAllocator<int32_t>(
        allocator, docs, "id",
        {100, 101, 102, 103, 200, 201, 202, 300, 301});
    extendMatchDocAllocator<int32_t>(
        allocator, docs, "group",
        {1, 1, 1, 1, 1, 1, 1, 2, 2});
    extendMatchDocAllocator<float>(
        allocator, docs, "score",
        {1.1, 0.9, 0.7, 0.5, 1.3, 0.3, 0.2, 1.2, 0.4});
    TablePtr input(new Table(docs, allocator));
    return input;
}

void DistinctTopNTvfFuncTest::testSuccess(
    const vector<string>& params,
    TablePtr input,
    const vector<int32_t>& expectIds,
    KernelScope scope)
{
    TvfFuncInitContext context;
    context.queryPool = _poolPtr.get();
    context.params = params;
    DistinctTopNTvfFunc tvfFunc;
    tvfFunc.setScope(scope);
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    checkOutputColumn(output, "id", expectIds);
}

TEST_F(DistinctTopNTvfFuncTest, testEnoughDistinct) {
    testSuccess(
        {"trigger,group", "-score", "6", "2", "3", "1"},
        fakeTable1(),
        {200, 300, 100, 101, 301, 201},
        KernelScope::FINAL
    );
}

TEST_F(DistinctTopNTvfFuncTest, testNotEnoughDistinct) {
    testSuccess(
        {"trigger,group", "-score", "6", "3", "3", "1"},
        fakeTable1(),
        {200, 300, 100, 101, 102, 301},
        KernelScope::FINAL
    );
}

TEST_F(DistinctTopNTvfFuncTest, testNotEnoughInput) {
    testSuccess(
        {"trigger,group", "-score", "12", "3", "3", "1"},
        fakeTable1(),
        {200, 300, 100, 101, 102, 103, 301, 201, 202},
        KernelScope::FINAL
    );
}

TEST_F(DistinctTopNTvfFuncTest, testPerGroupLimit0) {
    testSuccess(
        {"trigger,group", "-score", "6", "", "3", ""},
        fakeTable1(),
        {200, 300, 100, 101, 102, 103},
        KernelScope::FINAL
    );
    testSuccess(
        {"", "-score", "6", "2", "3", "1"},
        fakeTable1(),
        {200, 300, 100, 101, 102, 103},
        KernelScope::FINAL
    );
}

TEST_F(DistinctTopNTvfFuncTest, testPartialDistinct) {
    testSuccess(
        {"trigger,group", "-score", "12", "4", "6", "2"},
        fakeTable1(),
        {200, 300, 100, 101, 301, 201},
        KernelScope::PARTIAL
    );
    testSuccess(
        {"trigger,group", "-score", "12", "6", "6", "3"},
        fakeTable1(),
        {200, 300, 100, 101, 102, 301},
        KernelScope::PARTIAL
    );
    testSuccess(
        {"trigger,group", "-score", "24", "6", "12", "3"},
        fakeTable1(),
        {200, 300, 100, 101, 102, 103, 301, 201, 202},
        KernelScope::PARTIAL
    );
    testSuccess(
        {"trigger,group", "-score", "12", "", "6", ""},
        fakeTable1(),
        {200, 300, 100, 101, 102, 103},
        KernelScope::PARTIAL
    );
    testSuccess(
        {"", "-score", "12", "4", "6", "2"},
        fakeTable1(),
        {200, 300, 100, 101, 102, 103},
        KernelScope::PARTIAL
    );
}

TEST_F(DistinctTopNTvfFuncTest, testInit) {
    {
        TvfFuncInitContext context;
        DistinctTopNTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params = {"trigger_id", "-modified_score", "10", "1"};
        DistinctTopNTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params = {"trigger_id", "", "10", "2", "10", "2"};
        DistinctTopNTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params = {"trigger_id", "-modified_score", "10", "2", "20", "1"};
        DistinctTopNTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(10, tvfFunc._totalLimit);
        ASSERT_EQ(2, tvfFunc._perGroupLimit);
        ASSERT_EQ(10, tvfFunc._partTotalLimit);
        ASSERT_EQ(1, tvfFunc._partPerGroupLimit);
        vector<string> expectPVec = {"trigger_id"};
        vector<string> expectSVec = {"modified_score"};
        vector<bool> fVec = {true};
        ASSERT_EQ(expectPVec, tvfFunc._groupFields);
        ASSERT_EQ(expectSVec, tvfFunc._sortFields);
        ASSERT_EQ(fVec, tvfFunc._sortFlags);
    }
    {
        TvfFuncInitContext context;
        context.params = {"", "red_green,modified_score", "10", "2", "5", "-1"};
        DistinctTopNTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(10, tvfFunc._totalLimit);
        ASSERT_EQ(2, tvfFunc._perGroupLimit);
        ASSERT_EQ(5, tvfFunc._partTotalLimit);
        ASSERT_EQ(0, tvfFunc._partPerGroupLimit);
        vector<string> expectPVec = {};
        vector<string> expectSVec = {"red_green", "modified_score"};
        vector<bool> fVec = {false, false};
        ASSERT_EQ(expectPVec, tvfFunc._groupFields);
        ASSERT_EQ(expectSVec, tvfFunc._sortFields);
        ASSERT_EQ(fVec, tvfFunc._sortFlags);
    }
    {
        TvfFuncInitContext context;
        context.params = {"trigger_id,item_id", "+red_green,-modified_score", "10", "2", "5", "1"};
        DistinctTopNTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(10, tvfFunc._totalLimit);
        ASSERT_EQ(2, tvfFunc._perGroupLimit);
        ASSERT_EQ(5, tvfFunc._partTotalLimit);
        ASSERT_EQ(1, tvfFunc._partPerGroupLimit);
        vector<string> expectPVec = {"trigger_id", "item_id"};
        vector<string> expectSVec = {"red_green", "modified_score"};
        vector<bool> fVec = {false, true};
        ASSERT_EQ(expectPVec, tvfFunc._groupFields);
        ASSERT_EQ(expectSVec, tvfFunc._sortFields);
        ASSERT_EQ(fVec, tvfFunc._sortFlags);
    }
}

TEST_F(DistinctTopNTvfFuncTest, testdoComputeCreateComparatorFail) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(3);
    extendMatchDocAllocator<int32_t>(allocator, docs, "id", {3, 2, 1});
    extendMatchDocAllocator<int32_t>(allocator, docs, "value", {3, 3, 2});
    TablePtr input(new Table(docs, allocator));
    TvfFuncInitContext context;
    context.params = {"id", "value1", "2", "1", "2", "1"};
    context.queryPool = _poolPtr.get();
    DistinctTopNTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_FALSE(tvfFunc.doCompute(input, output));
}

TEST_F(DistinctTopNTvfFuncTest, testdoComputeGroupKeyFailed) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(2);
    extendMatchDocAllocator<int32_t>(allocator, docs, "id", {3, 2});
    extendMatchDocAllocator<int32_t>(allocator, docs, "value", {3, 2});
    TablePtr input(new Table(docs, allocator));
    TvfFuncInitContext context;
    context.params = {"id1", "value", "1", "1", "1", "1"};
    context.queryPool = _poolPtr.get();
    DistinctTopNTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr output;
    ASSERT_FALSE(tvfFunc.doCompute(input, output));
}

TEST_F(DistinctTopNTvfFuncTest, testRegTvfModels) {
    DistinctTopNTvfFuncCreator creator;
    iquan::TvfModels tvfModels;
    ASSERT_TRUE(creator.regTvfModels(tvfModels));
    ASSERT_EQ(1, tvfModels.functions.size());
}

END_HA3_NAMESPACE();
