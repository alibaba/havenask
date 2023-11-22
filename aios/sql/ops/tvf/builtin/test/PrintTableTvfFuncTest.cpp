#include "sql/ops/tvf/builtin/PrintTableTvfFunc.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "iquan/common/catalog/FunctionModel.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/VectorDocStorage.h"
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

class PrintTableTvfFuncTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
    TablePtr prepareInputTable(vector<int32_t> value);
};

void PrintTableTvfFuncTest::setUp() {}

void PrintTableTvfFuncTest::tearDown() {}

TablePtr PrintTableTvfFuncTest::prepareInputTable(vector<int32_t> value) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(value);
    _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", value);
    return Table::fromMatchDocs(docs, allocator);
}

TEST_F(PrintTableTvfFuncTest, testInit) {
    {
        TvfFuncInitContext context;
        PrintTableTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params.push_back("abc");
        PrintTableTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params.push_back("123");
        PrintTableTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(123, tvfFunc._count);
    }
}

TEST_F(PrintTableTvfFuncTest, testCompute) {
    TvfFuncInitContext context;
    context.params.push_back("123");
    PrintTableTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr input, output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));

    input = prepareInputTable({1, 2});
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output != nullptr);
    vector<int32_t> expects = {1, 2};
    checkOutputColumn<int32_t>(output, "id", expects);
}

TEST_F(PrintTableTvfFuncTest, testRegTvfModels) {
    PrintTableTvfFuncCreator creator;
    std::vector<iquan::FunctionModel> tvfModels;
    ASSERT_TRUE(creator.regTvfModels(tvfModels));
    ASSERT_EQ(1, tvfModels.size());
}

} // namespace sql
