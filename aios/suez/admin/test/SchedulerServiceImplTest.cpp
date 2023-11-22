#include <atomic>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-actions.h>
#include <gmock/gmock-spec-builders.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iostream>
#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <thread>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "carbon/proto/Carbon.pb.h"
#include "suez/admin/AdminOps.h"
#include "unittest/unittest.h"

namespace google {
namespace protobuf {
class Closure;
class RpcController;
} // namespace protobuf
} // namespace google

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib;
using namespace fslib::fs;
using namespace carbon;
using namespace carbon::proto;
using namespace http_arpc;
using namespace hippo;
using namespace cm_basic;

namespace suez {

class SchedulerServiceImplTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SchedulerServiceImplTest::setUp() {}

void SchedulerServiceImplTest::tearDown() {}

TEST_F(SchedulerServiceImplTest, testExtractStatusInfo) {
    map<string, carbon::GroupStatus> groupStatusMap;
    carbon::proto::RoleStatus roleStatus;
    auto nextRoleInfo = roleStatus.mutable_nextinstanceinfo();
    const string topoInfo =
        "enhanced_private_compute_123456789.default:1:0:3689812725:100:2400309353:-1:true|enhanced_private_compute_"
        "123456789.default_agg:1:0:3689812725:100:2400309353:-1:true|enhanced_private_compute_123456789.default_sql:1:"
        "0:3689812725:100:2400309353:-1:true|enhanced_private_compute_123456789.para_search_16:1:0:3689812725:100:"
        "2400309353:-1:true|enhanced_private_compute_123456789.para_search_2:1:0:3689812725:100:2400309353:-1:true|"
        "enhanced_private_compute_123456789.para_search_4:1:0:3689812725:100:2400309353:-1:true|enhanced_private_"
        "compute_123456789.para_search_8:1:0:3689812725:100:2400309353:-1:true|";
    JsonMap serviceInfo;
    JsonMap cm2Info;
    cm2Info["topo_info"] = topoInfo;
    serviceInfo["cm2"] = cm2Info;
    string serviceInfoStr = FastToJsonString(serviceInfo, true);
    {
        auto replicaNode = nextRoleInfo->add_replicanodes();
        replicaNode->set_isbackup(false);
        replicaNode->set_targetcustominfo("target_custom_info");
        replicaNode->set_custominfo("custom_info");
        replicaNode->set_userdefversion("123");
        replicaNode->set_serviceinfo(serviceInfoStr);
        replicaNode->set_ip("127.0.0.1");
        replicaNode->set_readyforcurversion(true);
    }
    {
        auto replicaNode = nextRoleInfo->add_replicanodes();
        replicaNode->set_isbackup(true);
    }
    groupStatusMap["g1"].roleStatuses["r1"] = roleStatus;

    JsonNodeRef::JsonMap status;
    SchedulerServiceImpl::extractStatusInfo(groupStatusMap, &status);
    // std::cout << FastToJsonString(status) << std::endl;
    auto g1 = getField(&status, "g1", JsonNodeRef::JsonMap());
    auto r1 = getField(&g1, "r1", JsonNodeRef::JsonArray());
    ASSERT_EQ(7, r1.size());

    vector<string> bizNameVec{
        "default", "default_agg", "default_sql", "para_search_16", "para_search_2", "para_search_4", "para_search_8"};
    for (size_t i = 0; i < r1.size(); ++i) {
        auto node = AnyCast<JsonNodeRef::JsonMap>(r1[i]);
        auto bizName = getField(&node, "biz_name", string());
        auto ip = getField(&node, "ip", string());
        auto partCnt = getField(&node, "part_count", int32_t(-1));
        auto partId = getField(&node, "part_id", int32_t(-1));
        auto version = getField(&node, "version", int64_t(-1));
        ASSERT_EQ("enhanced_private_compute_123456789." + bizNameVec[i], bizName);
        ASSERT_EQ("127.0.0.1", ip);
        ASSERT_EQ(0, partId);
        ASSERT_EQ(1, partCnt);
        ASSERT_EQ(3689812725, version);
    }
}

TEST_F(SchedulerServiceImplTest, testGetCm2TopoStr) {
    {
        string serviceInfo;
        string topoInfo = SchedulerServiceImpl::getCm2TopoStr(serviceInfo);
        EXPECT_EQ("", topoInfo);
    }
    {
        string serviceInfo = "{}";
        string topoInfo = SchedulerServiceImpl::getCm2TopoStr(serviceInfo);
        EXPECT_EQ("", topoInfo);
    }
    {
        string serviceInfo = "invalid";
        string topoInfo = SchedulerServiceImpl::getCm2TopoStr(serviceInfo);
        EXPECT_EQ("", topoInfo);
    }
    {
        string serviceInfo = R"({
"cm2": {}
})";
        string topoInfo = SchedulerServiceImpl::getCm2TopoStr(serviceInfo);
        EXPECT_EQ("", topoInfo);
    }
    {
        string serviceInfo = R"({
"cm2": {
"topo_info": "topo_info_str"
}
})";
        string topoInfo = SchedulerServiceImpl::getCm2TopoStr(serviceInfo);
        EXPECT_EQ("topo_info_str", topoInfo);
    }
    {
        string serviceInfo = R"({
"cm2": {
"topo_info": "role.default:1:0:3689812725:100:2400309353:-1:true|"
}
})";
        string topoInfo = SchedulerServiceImpl::getCm2TopoStr(serviceInfo);
        EXPECT_EQ("role.default:1:0:3689812725:100:2400309353:-1:true|", topoInfo);
    }
}

TEST_F(SchedulerServiceImplTest, testExtractNodeStatusInfo) {
    carbon::proto::ReplicaNode node;
    {
        string serviceInfo = R"({
"cm2": {
"topo_info": "role.default:1:0:3689812725:100:2400309353:-1:true|role.default_agg:1:0:3689812725:100:2400309353:-1:true|role.default_sql:1:0:3689812725:100:2400309353:-1:true|role.para_search_16:1:0:3689812725:100:2400309353:-1:true|role.para_search_2:1:0:3689812725:100:2400309353:-1:true|role.para_search_4:1:0:3689812725:100:2400309353:-1:true|role.para_search_8:1:0:3689812725:100:2400309353:-1:true|"
}
})";
        JsonNodeRef::JsonArray roleStatusInfo;
        node.set_serviceinfo(serviceInfo);
        SchedulerServiceImpl::extractNodeStatusInfo(node, &roleStatusInfo);
        EXPECT_EQ(7, roleStatusInfo.size());
        vector<string> bizNameVec{"role.default",
                                  "role.default_agg",
                                  "role.default_sql",
                                  "role.para_search_16",
                                  "role.para_search_2",
                                  "role.para_search_4",
                                  "role.para_search_8"};
        for (size_t i = 0; i < 7; ++i) {
            auto localTopoInfo = AnyCast<JsonNodeRef::JsonMap>(roleStatusInfo[i]);
            string bizName = getField(&localTopoInfo, "biz_name", string());
            EXPECT_EQ(bizName, bizNameVec[i]);
        }
    }
    {
        string serviceInfo = R"({
"cm2": {
"topo_info": "{}"
}
})";
        JsonNodeRef::JsonArray roleStatusInfo;
        node.set_serviceinfo(serviceInfo);
        SchedulerServiceImpl::extractNodeStatusInfo(node, &roleStatusInfo);
        EXPECT_EQ(0, roleStatusInfo.size());
    }
}

TEST_F(SchedulerServiceImplTest, testParseCm2TopoStr) {
    string bizName;
    int32_t partCnt = 0;
    int32_t partId = 0;
    int64_t version = 0;
    int32_t grpcPort = -1;
    bool supportHeartbeat = false;
    {
        string topoInfo;
        ASSERT_FALSE(SchedulerServiceImpl::parseCm2TopoStr(
            topoInfo, bizName, partCnt, partId, version, grpcPort, supportHeartbeat));
    }
    {
        string topoInfo = "invalid";
        ASSERT_FALSE(SchedulerServiceImpl::parseCm2TopoStr(
            topoInfo, bizName, partCnt, partId, version, grpcPort, supportHeartbeat));
    }
    {
        string topoInfo = "invalid:invalid";
        ASSERT_FALSE(SchedulerServiceImpl::parseCm2TopoStr(
            topoInfo, bizName, partCnt, partId, version, grpcPort, supportHeartbeat));
    }
    {
        string topoInfo = "a:b:c:d:e:f:g:h";
        ASSERT_FALSE(SchedulerServiceImpl::parseCm2TopoStr(
            topoInfo, bizName, partCnt, partId, version, grpcPort, supportHeartbeat));
    }
    {
        string topoInfo = "biz_name:1:0:3689812725:100:2400309353:-1:invalid_bool";
        ASSERT_FALSE(SchedulerServiceImpl::parseCm2TopoStr(
            topoInfo, bizName, partCnt, partId, version, grpcPort, supportHeartbeat));
    }
    {
        string topoInfo = "biz_name:1:0:3689812725:100:2400309353:-1:true";
        ASSERT_TRUE(SchedulerServiceImpl::parseCm2TopoStr(
            topoInfo, bizName, partCnt, partId, version, grpcPort, supportHeartbeat));
        EXPECT_EQ("biz_name", bizName);
        EXPECT_EQ(1, partCnt);
        EXPECT_EQ(0, partId);
        EXPECT_EQ(3689812725, version);
        EXPECT_EQ(-1, grpcPort);
        EXPECT_EQ(true, supportHeartbeat);
    }
}

} // namespace suez
