#include <algorithm>
#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/common.h"
#include "navi/engine/KernelConfigContext.h"
#include "sql/common/common.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/ops/tvf/TvfWrapperR.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace navi;
using namespace matchdoc;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class TvfWrapperTest : public OpTestBase {
public:
    void setUp();
    void tearDown();

private:
    void prepareTable() {
        _poolPtr.reset(new autil::mem_pool::Pool());
        _allocator.reset(new matchdoc::MatchDocAllocator(_poolPtr));
        vector<MatchDoc> docs = _allocator->batchAllocate(2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<char>(
            _allocator, docs, "mc", {{'c', '1'}, {'2'}}));
        _table.reset(new Table(docs, _allocator));
    }

private:
    MatchDocAllocatorPtr _allocator;
    TablePtr _table;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, TvfWrapperTest);

void TvfWrapperTest::setUp() {
    prepareTable();
}

void TvfWrapperTest::tearDown() {}

TEST_F(TvfWrapperTest, testTvfInitParamInit) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["invocation"] = ParseJson(
        R"json({"op" : "printTableTvf", "params" : ["1", "@table#0"], "type":"TVF"})json");
    attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
    attributeMap["scope"] = string("NORMAL");
    attributeMap["op_id"] = Any(100);

    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    KernelConfigContextPtr ctx;
    ASSERT_NO_FATAL_FAILURE(makeConfigCtx(jsonStr, ctx));

    TvfInitParam param;
    ASSERT_TRUE(param.initFromJson(*ctx));
    ASSERT_THAT(param.outputFields, testing::ElementsAre("id", "mc"));
    ASSERT_THAT(param.outputFieldsType, testing::ElementsAre("int32", "multi_int32"));
    ASSERT_EQ(KernelScope::NORMAL, param.scope);
    ASSERT_EQ(100, param.opId);
}

TEST_F(TvfWrapperTest, testTvfInitParamInit_InvocationTypeNotTvf) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["invocation"] = ParseJson(
        R"json({"op" : "printTableTvf", "params" : ["1", "@table#0"], "type":"udf"})json");
    attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
    attributeMap["scope"] = string("NORMAL");
    attributeMap["op_id"] = Any(100);

    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    KernelConfigContextPtr ctx;
    ASSERT_NO_FATAL_FAILURE(makeConfigCtx(jsonStr, ctx));

    TvfInitParam param;
    ASSERT_FALSE(param.initFromJson(*ctx));
}

TEST_F(TvfWrapperTest, testCheckOutputFailed) {
    TvfWrapperR wrapper;
    wrapper._initParam.outputFields = {"test"};
    ASSERT_FALSE(wrapper.checkOutputTable(_table));
}

TEST_F(TvfWrapperTest, testCheckOutput) {
    TvfWrapperR wrapper;
    wrapper._initParam.outputFields = {"mc"};
    ASSERT_TRUE(wrapper.checkOutputTable(_table));
}

} // namespace sql
