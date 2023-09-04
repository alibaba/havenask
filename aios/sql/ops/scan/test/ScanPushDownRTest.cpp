#include "sql/ops/scan/ScanPushDownR.h"

#include <iosfwd>
#include <string>

#include "autil/legacy/exception.h"
#include "navi/common.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/ops/test/OpTestBase.h"
#include "unittest/unittest.h"

using namespace std;
using namespace navi;
using namespace autil::legacy;

namespace sql {

class ScanPushDownRTest : public OpTestBase {
public:
    ScanPushDownRTest();
    ~ScanPushDownRTest();
};

ScanPushDownRTest::ScanPushDownRTest() {}

ScanPushDownRTest::~ScanPushDownRTest() {}

TEST_F(ScanPushDownRTest, testInitPushDownOpConfig) {
    std::string configStr = R"json({
"push_down_ops": [
    {
        "attrs":{
            "block":false,
            "reserve_max_count": 100,
            "invocation":{
               "op":"sortTvf",
               "params":[
                   "-attr1",
                   "2",
                   "@table#0"
               ],
               "type":"TVF"
           },
           "output_fields":[
               "$attr1"
           ],
           "output_fields_type":[
               "INTEGER"
           ],
           "scope":"NORMAL",
           "top_distribution":{
               "partition_cnt":0
           },
           "top_properties":{
               "enable_shuffle":false
           }
        },
        "op_name":"TableFunctionScanOp"
    },
    {
        "attrs":{
            "output_field_exprs":{
                "$score":{
                    "op":"+",
                    "params":[
                        "$attr1",
                        1
                    ],
                    "type":"OTHER"
                }
            },
            "output_fields":[
                "$attr1",
                "$score"
            ],
            "output_fields_type":[
                "INTEGER",
                "INTEGER"
            ]
        },
        "op_name":"CalcOp"
    },
    {
        "attrs":{
            "block":false,
            "reserve_max_count": 100,
            "invocation":{
               "op":"sortTvf",
               "params":[
                   "-attr1",
                   "2",
                   "@table#0"
               ],
               "type":"TVF"
           },
           "output_fields":[
               "$attr1"
           ],
           "output_fields_type":[
               "INTEGER"
           ],
           "scope":"NORMAL",
           "top_distribution":{
               "partition_cnt":0
           },
           "top_properties":{
               "enable_shuffle":false
           }
        },
        "op_name":"TableFunctionScanOp"
    }
]
})json";
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(configStr);
    auto *pushDownR = naviRHelper->getOrCreateRes<ScanPushDownR>();
    ASSERT_TRUE(pushDownR);
    ASSERT_EQ(100, pushDownR->_reserveMaxCount);
}

TEST_F(ScanPushDownRTest, testInitPushDownOpConfig_InvalidTvfConfig) {
    std::string configStr = R"json({
"push_down_ops": [
    {
        "attrs": {
            "condition": {"op":">", "params":[
                "$attr1", 2
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    },
    {
        "attrs":{
            "block":false,
            "reserve_max_count": 100,
            "invocation":{
               "op":"sortTvf",
               "params":[
                   "-attr1",
                   "2",
                   "@table#0"
               ],
               "type":"invalid"
           },
           "output_fields":[
               "$attr1"
           ],
           "output_fields_type":[
               "INTEGER"
           ],
           "scope":"NORMAL",
           "top_distribution":{
               "partition_cnt":0
           },
           "top_properties":{
               "enable_shuffle":false
           }
        },
        "op_name":"TableFunctionScanOp"
    },
    {
        "attrs":{
            "output_field_exprs":{
                "$score":{
                    "op":"+",
                    "params":[
                        "$attr1",
                        1
                    ],
                    "type":"OTHER"
                }
            },
            "output_fields":[
                "$attr1",
                "$score"
            ],
            "output_fields_type":[
                "INTEGER",
                "INTEGER"
            ]
        },
        "op_name":"CalcOp"
    }
]
})json";
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(configStr);
    auto *pushDownR = naviRHelper->getOrCreateRes<ScanPushDownR>();
    ASSERT_FALSE(pushDownR);
}

TEST_F(ScanPushDownRTest, testInitPushDownOpConfig_UnsupportedOp) {
    std::string configStr = R"json({
"push_down_ops": [
    {
        "attrs": {
            "condition": {"op":">", "params":[
                "$attr1", 2
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    },
    {
        "attrs":{
            "block":false,
            "reserve_max_count": 100,
            "invocation":{
               "op":"sortTvf",
               "params":[
                   "-attr1",
                   "2",
                   "@table#0"
               ],
               "type":"TVF"
           },
           "output_fields":[
               "$attr1"
           ],
           "output_fields_type":[
               "INTEGER"
           ],
           "scope":"NORMAL",
           "top_distribution":{
               "partition_cnt":0
           },
           "top_properties":{
               "enable_shuffle":false
           }
        },
        "op_name":"MergeOp"
    }
]
})json";
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(configStr);
    auto *pushDownR = naviRHelper->getOrCreateRes<ScanPushDownR>();
    ASSERT_FALSE(pushDownR);
}

} // namespace sql
