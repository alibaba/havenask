#include <iosfwd>

#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "build_service/analyzer/Token.h"
#include "ha3/turing/common/ModelConfig.h"
#include "navi/common.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/expression/util/FieldBoost.h"
#include "table/Row.h"

using namespace std;
using namespace suez::turing;
using namespace build_service::analyzer;
using namespace navi;
using namespace autil;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace isearch::turing;

namespace sql {

class ScanBaseTest : public OpTestBase {
public:
    ScanBaseTest();
    ~ScanBaseTest();

public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
    }
};

ScanBaseTest::ScanBaseTest() {}

ScanBaseTest::~ScanBaseTest() {}

// TEST_F(ScanBaseTest, testScanBaseInit) {
//     autil::legacy::json::JsonMap attributeMap;
//     attributeMap["table_type"] = string("normal");
//     attributeMap["table_name"] = _tableName;
//     attributeMap["batch_size"] = Any(100);
//     attributeMap["limit"] = Any(1000);
//     string jsonStr = autil::legacy::FastToJsonString(attributeMap);
//     auto *naviRHelper = getNaviRHelper();
//     naviRHelper->kernelConfig(jsonStr);
//     auto param = naviRHelper->getOrCreateRes<ScanInitParamR>();
//     ASSERT_TRUE(param);
//     ScanBase scanBase;
//     scanBase._scanInitParamR = param;
//     ASSERT_TRUE(scanBase.doInit());
//     ASSERT_EQ(100, scanBase._batchSize);
//     ASSERT_EQ(1000, scanBase._limit);
// }

} // namespace sql
