#include "sql/ops/tvf/builtin/IdentityTvfFunc.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <vector>

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
using namespace table;
using namespace testing;
using namespace matchdoc;

namespace sql {

class IdentityTvfFuncTest : public OpTestBase {
protected:
    TablePtr prepareInputTable(vector<int32_t> value) {
        MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
        vector<MatchDoc> docs = allocator->batchAllocate(value);
        _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "id", value);
        TablePtr table;
        table.reset(new Table(docs, allocator));
        return table;
    }
};

TEST_F(IdentityTvfFuncTest, testCompute) {
    TvfFuncInitContext context;
    IdentityTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr input, output;
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_EQ(input.get(), output.get());

    input = prepareInputTable({1, 2});
    ASSERT_TRUE(tvfFunc.compute(input, true, output));
    ASSERT_TRUE(output != nullptr);
    vector<int32_t> expects = {1, 2};
    checkOutputColumn<int32_t>(output, "id", expects);
}

} // namespace sql
