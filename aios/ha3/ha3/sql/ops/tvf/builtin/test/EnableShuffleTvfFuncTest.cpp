#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/tvf/builtin/EnableShuffleTvfFunc.h"

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class EnableShuffleTvfFuncTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
    TablePtr  prepareInputTable(vector<int32_t> value);
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(builtin, EnableShuffleTvfFuncTest);

void EnableShuffleTvfFuncTest::setUp() {
}

void EnableShuffleTvfFuncTest::tearDown() {
}

TablePtr EnableShuffleTvfFuncTest::prepareInputTable(vector<int32_t> value) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(value);
    extendMatchDocAllocator<int32_t>(allocator, docs, "id", value);
    TablePtr table;
    table.reset(new Table(docs, allocator));
    return table;
}

TEST_F(EnableShuffleTvfFuncTest, testCompute) {
    TvfFuncInitContext context;
    context.params.push_back("123");
    EnableShuffleTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr input, output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));

    input = prepareInputTable({1,2});
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output != nullptr);
    vector<int32_t> expects = {1, 2};
    checkOutputColumn<int32_t>(output, "id", expects);
}

TEST_F(EnableShuffleTvfFuncTest, testRegTvfModels) {
    EnableShuffleTvfFuncCreator creator;
    iquan::TvfModels tvfModels;
    ASSERT_TRUE(creator.regTvfModels(tvfModels));
    ASSERT_EQ(1, tvfModels.functions.size());
}

END_HA3_NAMESPACE();
