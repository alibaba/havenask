#include <algorithm>
#include <memory>
#include <mutex>
#include <ostream>
#include <rapidjson/document.h>
#include <string>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "iquan/jni/DynamicParams.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/IquanEnv.h"
#include "iquan/jni/JvmType.h"
#include "iquan/jni/SqlPlan.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/MetricsReporterR.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/data/SqlGraphData.h"
#include "sql/data/SqlPlanData.h"
#include "sql/data/SqlQueryRequest.h"
#include "sql/data/SqlRequestData.h"
#include "sql/resource/IquanR.h"
#include "sql/resource/SqlConfigResource.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace iquan;
using namespace navi;

namespace sql {
static std::once_flag iquanFlag;
class PlanTransformKernelTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(, PlanTransformKernelTest);

void PlanTransformKernelTest::setUp() {
    std::call_once(iquanFlag, [&]() {
        // make iquan search for iquan.jar from workdir
        autil::EnvUtil::setEnv(BINARY_PATH, ".");
        Status status = IquanEnv::jvmSetup(jt_hdfs_jvm, {}, "");
        ASSERT_TRUE(status.ok()) << "can not start jvm" << std::endl << status.errorMessage();
        autil::EnvUtil::setEnv(BINARY_PATH, "");
    });
}

void PlanTransformKernelTest::tearDown() {}

TEST_F(PlanTransformKernelTest, testSimple) {
    string iquanConfigStr = string(R"json({"iquan_jni_config":{}, "iquan_client_config":{}})json");
    string sqlConfigStr
        = string(R"json({"iquan_sql_config":{"catalog_name":"default", "db_name":"default"}})json");
    navi::KernelTesterBuilder builder;
    builder.resourceConfig(IquanR::RESOURCE_ID, iquanConfigStr);
    builder.resourceConfig(SqlConfigResource::RESOURCE_ID, sqlConfigStr);
    builder.kernel("sql.PlanTransformKernel");
    builder.input("input0");
    builder.input("input1");
    builder.output("output0");
    kmonitor::MetricsReporterPtr metricsReporter(new kmonitor::MetricsReporter("", {}, ""));
    kmonitor::MetricsReporterRPtr metricsReporterR(new kmonitor::MetricsReporterR(metricsReporter));
    builder.resource(metricsReporterR);
    auto tester = builder.build();
    ASSERT_TRUE(tester);
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
    SqlPlanDataPtr inputData(new SqlPlanData(iquan::IquanDqlRequestPtr(), sqlPlan));
    inputData->_iquanSqlRequest.reset(new iquan::IquanDqlRequest());
    ASSERT_TRUE(tester->setInput("input0", inputData, true));

    SqlQueryRequestPtr sqlQuery(new SqlQueryRequest());
    SqlRequestDataPtr inputData2(new SqlRequestData(sqlQuery));
    ASSERT_TRUE(tester->setInput("input1", inputData2, true));

    ASSERT_TRUE(tester->compute());
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester->getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    ASSERT_TRUE(eof);
    auto sqlGraphData = dynamic_pointer_cast<SqlGraphData>(outputData);
    ASSERT_TRUE(sqlGraphData != nullptr);
    ASSERT_TRUE(sqlGraphData->getGraphDef() != nullptr);
    ASSERT_EQ(1, sqlGraphData->getGraphDef()->sub_graphs_size())
        << sqlGraphData->getGraphDef()->ShortDebugString();
}

} // namespace sql
