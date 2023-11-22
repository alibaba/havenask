#include "sql/ops/tvf/builtin/TransRowsTvfFunc.h"

#include "autil/MultiValueType.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/VectorDocStorage.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace table;
using namespace matchdoc;

namespace sql {

class TransRowsTvfFuncTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
    TablePtr prepareInputTable() {
        MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
        vector<MatchDoc> docs = allocator->batchAllocate(8);
        vector<int32_t> nid = {1, 2, 3, 4, 5, 6, 7, 8};
        vector<std::string> brand_price = {
            "huawei:4599", "xiaomi:2999:red", "oppo:3799", "apple:", "samsung", ":6999", ":", ""};
        _matchDocUtil.extendMatchDocAllocator<int32_t>(allocator, docs, "nid", nid);
        _matchDocUtil.extendMatchDocAllocator(allocator, docs, "brand_price", brand_price);
        return Table::fromMatchDocs(docs, allocator);
    }
};

void TransRowsTvfFuncTest::setUp() {}

void TransRowsTvfFuncTest::tearDown() {}

TEST_F(TransRowsTvfFuncTest, testInit) {
    // expect 2 params
    {
        TvfFuncInitContext context;
        TransRowsTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params = {"brand_price", ":", "3"};
        TransRowsTvfFunc tvfFunc;
        ASSERT_FALSE(tvfFunc.init(context));
    }
    {
        TvfFuncInitContext context;
        context.params = {"brand_price", ""};
        TransRowsTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(tvfFunc._multiValueSep, ":");
    }
    {
        TvfFuncInitContext context;
        context.params = {"brand_price", ";"};
        TransRowsTvfFunc tvfFunc;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(tvfFunc._multiValueSep, ";");
    }
}

TEST_F(TransRowsTvfFuncTest, testComputeSuccess) {
    TvfFuncInitContext context;
    context.params = {"brand_price", ":"};
    context.queryPool = _poolPtr.get();
    TransRowsTvfFunc tvfFunc;
    ASSERT_TRUE(tvfFunc.init(context));
    TablePtr input = prepareInputTable();
    TablePtr output;
    ASSERT_EQ(true, tvfFunc.compute(input, true, output));
    ASSERT_NE(nullptr, output.get());
    checkOutputColumn<int32_t>(output, "nid", {1, 2, 3, 4, 5, 6, 7, 8});
    checkOutputColumn(
        output,
        "brand_price",
        {"huawei:4599", "xiaomi:2999:red", "oppo:3799", "apple:", "samsung", ":6999", ":", ""});
    checkOutputColumn(
        output, "trans_cola", {"huawei", "xiaomi", "oppo", "apple", "samsung", "", "", ""});
    checkOutputColumn(output, "trans_colb", {"4599", "2999:red", "3799", "", "", "6999", "", ""});
}

} // namespace sql
