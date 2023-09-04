#include "sql/config/ExternalTableConfigR.h"

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/fast_jsonizable_dec.h"
#include "multi_call/common/ErrorCode.h"
#include "multi_call/config/MultiCallConfig.h"
#include "navi/common.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/common/common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::legacy;
using namespace multi_call;
using namespace navi;

namespace sql {

static const std::string exampleConfig = R"json({
    "gig_config": {
        "subscribe": {
            "cm2_configs": [
                {
                    "cm2_part": [
                        "biz_order_seller_online"
                    ],
                    "zk_host": "search-zk-cm2-client-na630cloud.alibaba-inc.com:2187",
                    "zk_path": "/cm_server_common"
                }
            ],
            "local": [
                {
                    "biz_name": "biz_order_search.external",
                    "grpc_port": 34493,
                    "http_port": -1,
                    "ip": "11.167.227.7",
                    "part_count": 1,
                    "part_id": 0,
                    "weight": 100
                }
            ]
        },
        "flow_control": {
            "biz_order_seller_online.external": {
                "min_weight": 0,
                "probe_percent": 0.05
            }
        }
    },
    "service_config": {
        "biz_order_seller_online.external": {
            "type": "HA3SQL",
            "engine_version": "3.12.0",
            "timeout_threshold_ms": 100,
            "misc_config": {
                "auth_token": "token_xxx",
                "auth_key": "key_xxx",
                "auth_enabled": false
            }
        }
    },
    "table_config": {
        "biz_order_seller_online_a": {
            "schema_file": "",
            "database_name": "biz_order_seller_online",
            "table_name": "a",
            "service_name": "biz_order_seller_online.external"
        },
        "biz_order_seller_online_b": {
            "schema_file": "",
            "database_name": "share1",
            "table_name": "b",
            "service_name": "biz_order_seller_online.external"
        }
    }
    })json";

class ExternalTableConfigRTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

    void createWithJsonStr(std::shared_ptr<ExternalTableConfigR> &configR,
                           const std::string &configStr) {
        ASSERT_TRUE(_naviRHelper.config(ExternalTableConfigR::RESOURCE_ID, configStr));
        configR = _naviRHelper.getOrCreateResPtr<ExternalTableConfigR>();
        ASSERT_TRUE(configR);
    }

private:
    NaviResourceHelper _naviRHelper;
};

void ExternalTableConfigRTest::setUp() {}

void ExternalTableConfigRTest::tearDown() {}

TEST_F(ExternalTableConfigRTest, testDumpToJson) {
    auto configStr = R"json({
            "type": "HA3SQL",
            "engine_version": "3.12.0",
            "timeout_threshold_ms": 100,
            "misc_config": {
                "auth_token": "token_xxx",
                "auth_key": "key_xxx",
                "auth_enabled": false
            }
    })json";
    ServiceConfig config;
    FastFromJsonString(config, configStr);
    auto output = FastToJsonString(config);
    ASSERT_NE(std::string::npos, output.find("misc_config"));
}

TEST_F(ExternalTableConfigRTest, testParse) {
    std::shared_ptr<ExternalTableConfigR> configR;
    ASSERT_NO_FATAL_FAILURE(createWithJsonStr(configR, exampleConfig));
    std::vector<multi_call::Cm2Config> cm2Configs = configR->gigConfig.subscribeConfig.cm2Configs;
    // ASSERT_TRUE(configR->gigConfig.subscribeConfig.getCm2Configs(cm2Configs));
    ASSERT_EQ(1, cm2Configs.size());
    ASSERT_EQ("search-zk-cm2-client-na630cloud.alibaba-inc.com:2187", cm2Configs[0].zkHost);
    ASSERT_EQ("/cm_server_common", cm2Configs[0].zkPath);

    auto &mcFlowConfigMap = configR->gigConfig.flowConfigMap;
    auto it = mcFlowConfigMap.find("biz_order_seller_online.external");
    ASSERT_NE(mcFlowConfigMap.end(), it);
    ASSERT_NE(nullptr, it->second);
    ASSERT_NEAR(0.05, it->second->probePercent, 1e-5);
    ASSERT_NEAR(0, it->second->minWeight, 1e-5);

    auto serviceIter = configR->serviceConfigMap.find("biz_order_seller_online.external");
    ASSERT_NE(configR->serviceConfigMap.end(), serviceIter);
    ASSERT_EQ(EXTERNAL_TABLE_TYPE_HA3SQL, serviceIter->second.type);
    ASSERT_EQ("3.12.0", serviceIter->second.engineVersion);
    ASSERT_EQ(100, serviceIter->second.timeoutThresholdMs);
    auto *ha3MiscConfig
        = dynamic_cast<Ha3ServiceMiscConfig *>(serviceIter->second.miscConfig.get());
    ASSERT_NE(nullptr, ha3MiscConfig);
    ASSERT_EQ("token_xxx", ha3MiscConfig->authToken);
    ASSERT_EQ("key_xxx", ha3MiscConfig->authKey);
    ASSERT_FALSE(ha3MiscConfig->authEnabled);
}

TEST_F(ExternalTableConfigRTest, testParse_Error_InvalidType) {
    auto configStr = R"json({
            "type": "aaa",
            "engine_version": "3.12.0",
            "timeout_threshold_ms": 100,
            "misc_config": {
                "auth_token": "token_xxx",
                "auth_key": "key_xxx",
                "auth_enabled": false
            }
    })json";
    bool hasException = false;
    ServiceConfig config;
    try {
        FastFromJsonString(config, configStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        hasException = true;
        ASSERT_EQ("ParameterInvalidException", e.GetClassName());
        ASSERT_EQ("Invalid table type: aaa", e.GetMessage());
    }
    ASSERT_TRUE(hasException);
}

} // namespace sql
