#include "sql/data/SqlPlanType.h"

#include <algorithm>
#include <engine/TypeContext.h>
#include <iosfwd>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "iquan/common/Common.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "iquan/jni/DynamicParams.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/SqlPlan.h"
#include "navi/common.h"
#include "navi/util/NaviTestPool.h"
#include "sql/data/SqlPlanData.h"
#include "unittest/unittest.h"

using namespace std;
using namespace iquan;
using namespace navi;
using namespace autil;
using namespace testing;

namespace sql {

class SqlPlanTypeTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(data, SqlPlanTypeTest);

void SqlPlanTypeTest::setUp() {}

void SqlPlanTypeTest::tearDown() {}

TEST_F(SqlPlanTypeTest, testSimple) {
    SqlPlanPtr sqlPlan(new SqlPlan());
    std::string planOpStr = R"json(
{
    "id" : 0,
    "op_name" : "ScanKernel",
    "inputs" : {
    },
    "attrs" : {
        "table_name" : ""
    }
}
)json";
    std::shared_ptr<autil::legacy::RapidDocument> documentPtr(new autil::legacy::RapidDocument);
    std::string newStr = planOpStr + IQUAN_DYNAMIC_PARAMS_SIMD_PADDING;
    documentPtr->Parse(newStr.c_str());
    PlanOp planOp;
    Status status = Utils::fromRapidValue(planOp, documentPtr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    sqlPlan->opList.push_back(planOp);

    ASSERT_TRUE(sqlPlan->innerDynamicParams.empty());
    SqlPlanDataPtr sqlPlanData(new SqlPlanData(iquan::IquanDqlRequestPtr(), sqlPlan));

    autil::mem_pool::PoolPtr pool(new autil::mem_pool::PoolAsan);
    DataBuffer dataBuffer(2048, pool.get());
    TypeContext ctx(dataBuffer);
    SqlPlanType type;
    // TODO support sql plan serialize
    //  ASSERT_EQ(navi::TEC_NONE, type.serialize(ctx, sqlPlanData));

    // DataPtr data;
    // ASSERT_EQ(navi::TEC_NONE, type.deserialize(ctx, data));
    // auto sqlPlanData2 = dynamic_pointer_cast<SqlPlanData>(data);
    // ASSERT_TRUE(sqlPlanData2 != nullptr);
    // ASSERT_TRUE(sqlPlanData2->_iquanSqlRequest == nullptr);
    // ASSERT_EQ(1, sqlPlanData2->getSqlPlan()->opList.size());
}

} // namespace sql
