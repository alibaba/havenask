#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/tvf/TvfFunc.h"

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class TvfFuncTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
public:
    TablePtr  prepareInputTable(vector<int32_t> value);
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, TvfFuncTest);

void TvfFuncTest::setUp() {
}

void TvfFuncTest::tearDown() {
}

TablePtr  TvfFuncTest::prepareInputTable(vector<int32_t> value) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(value);
    extendMatchDocAllocator<int32_t>(allocator, docs, "id", value);
    TablePtr table;
    table.reset(new Table(docs, allocator));
    return table;
}

class MyTvfFunc : public OnePassTvfFunc {
public:
    bool init(TvfFuncInitContext &context) override {
        return true;
    }
    bool doCompute(const TablePtr &input, TablePtr &output) override {
        output = input;
        return true;
    }
};

TEST_F(TvfFuncTest, testOnePassCompute) {
    {
        MyTvfFunc myTvf;
        TablePtr input;
        TablePtr output;
        ASSERT_TRUE(myTvf.compute(input, false, output));
        ASSERT_TRUE(output == nullptr);
        input = prepareInputTable({1});
        ASSERT_TRUE(myTvf.compute(input, false, output));
        ASSERT_TRUE(output == nullptr);
        input = prepareInputTable({2});
        ASSERT_TRUE(myTvf.compute(input, true, output));
        ASSERT_TRUE(output != nullptr);
        vector<int32_t> expects = {1, 2};
        checkOutputColumn<int32_t>(output, "id", expects);
    }
    {
        MyTvfFunc myTvf;
        TablePtr input;
        TablePtr output;
        input = prepareInputTable({1});
        ASSERT_TRUE(myTvf.compute(input, false, output));
        ASSERT_TRUE(output == nullptr);
        ASSERT_TRUE(myTvf.compute(nullptr, true, output));
        ASSERT_TRUE(output != nullptr);
        vector<int32_t> expects = {1};
        checkOutputColumn<int32_t>(output, "id", expects);
    }
    {
        MyTvfFunc myTvf;
        TablePtr input;
        TablePtr output;
        ASSERT_FALSE(myTvf.compute(input, true, output));
    }


}

END_HA3_NAMESPACE();
