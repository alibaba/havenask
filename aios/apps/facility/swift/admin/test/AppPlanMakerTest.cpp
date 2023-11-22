#include "swift/admin/AppPlanMaker.h"

#include <algorithm>
#include <cstddef>
#include <gmock/gmock-matchers.h>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "hippo/DriverCommon.h"
#include "master_framework/RolePlan.h"
#include "swift/common/PathDefine.h"
#include "swift/config/AdminConfig.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace hippo;
using namespace swift::config;
using namespace swift::protocol;

namespace swift {
namespace admin {

class AppPlanMakerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    void
    innerTestForMakeAppPlan(const string &roleName, const string &configPath, AppPlan &plan, BrokerExclusiveLevel el);
};

void AppPlanMakerTest::setUp() {}

void AppPlanMakerTest::tearDown() {}

TEST_F(AppPlanMakerTest, testLoadConfigFailed) {
    {
        AppPlanMaker maker;
        EXPECT_FALSE(maker.init(GET_TEST_DATA_PATH() + "app_plan_maker_test/empty_swift_conf/", ""));
    }
    {
        AppPlanMaker maker;
        EXPECT_FALSE(maker.init(GET_TEST_DATA_PATH() + "app_plan_maker_test/empty_hippo_json/", ""));
    }
    {
        AppPlanMaker maker;
        EXPECT_FALSE(maker.init(GET_TEST_DATA_PATH() + "app_plan_maker_test/invalid_hippo_json", ""));
    }
    {
        AppPlanMaker maker;
        EXPECT_TRUE(maker.init(GET_TEST_DATA_PATH() + "app_plan_maker_test/normal_conf", ""));
    }
}

TEST_F(AppPlanMakerTest, testValidate) {
    {
        AppPlanMaker maker;
        ASSERT_TRUE(maker.init(GET_TEST_DATA_PATH() + "app_plan_maker_test/no_process_info/", ""));
        EXPECT_FALSE(maker.validate());
    }
    {
        AppPlanMaker maker;
        ASSERT_TRUE(maker.init(GET_TEST_DATA_PATH() + "app_plan_maker_test/resource_empty/", ""));
        EXPECT_FALSE(maker.validate());
    }
}

TEST_F(AppPlanMakerTest, testInit) {
    string configPath = GET_TEST_DATA_PATH() + "app_plan_maker_test/normal_conf";
    AppPlanMaker maker;
    ASSERT_TRUE(maker.init(configPath, "11"));

    ASSERT_EQ(size_t(2), maker._packageInfos.size());
    EXPECT_EQ(PackageInfo::RPM, maker._packageInfos[0].type);
    EXPECT_EQ(string("swift_package"), maker._packageInfos[0].URI);
    EXPECT_EQ(PackageInfo::ARCHIVE, maker._packageInfos[1].type);
    EXPECT_EQ(string("other_packages"), maker._packageInfos[1].URI);

    EXPECT_TRUE(maker._defaultRolePlan.slotResources.empty());
    EXPECT_EQ(int32_t(1), maker._defaultRolePlan.count);
    EXPECT_EQ(size_t(1), maker._defaultRolePlan.processInfos.size());

    ASSERT_EQ(size_t(2), maker._groupResourceMap.size());
    ASSERT_TRUE(maker._groupResourceMap.find("admin") != maker._groupResourceMap.end());
    ASSERT_TRUE(maker._groupResourceMap.find("broker") != maker._groupResourceMap.end());

    ASSERT_EQ(string("11"), maker.getRoleVersion());
    ASSERT_EQ(configPath, maker.getConfigPath());
    ASSERT_EQ(size_t(4), maker.getRoleCount(RoleType::ROLE_TYPE_BROKER));
    vector<string> roleNames = maker.getRoleName(RoleType::ROLE_TYPE_ALL);
    ASSERT_EQ(size_t(4), roleNames.size());
    for (size_t i = 0; i < 4; i++) {
        string expectRoleName =
            string("default##broker_") + autil::StringUtil::toString(i) + "_" + maker.getRoleVersion();
        EXPECT_EQ(expectRoleName, roleNames[i]);
    }
}

TEST_F(AppPlanMakerTest, testMakeAppPlan) {
    string configPath = GET_TEST_DATA_PATH() + "app_plan_maker_test/normal_conf";
    AppPlanMaker maker;
    ASSERT_TRUE(maker.init(configPath, "11"));

    AppPlan plan;
    maker.makeBrokerRolePlan(plan);
    ASSERT_EQ(size_t(0), plan.packageInfos.size());
    ASSERT_EQ(size_t(4), plan.rolePlans.size());

    vector<string> roleNames = maker.getRoleName(RoleType::ROLE_TYPE_ALL);
    ASSERT_EQ(size_t(4), roleNames.size());
    for (size_t i = 0; i < roleNames.size(); i++) {
        innerTestForMakeAppPlan(roleNames[i], configPath, plan, maker._adminConfig->getExclusiveLevel());
    }
}

TEST_F(AppPlanMakerTest, testMakeAppPlanGroupExclusive) {
    string configPath = GET_TEST_DATA_PATH() + "app_plan_maker_test/normal_conf_group_exclusive";
    AppPlanMaker maker;
    ASSERT_TRUE(maker.init(configPath, "11"));

    AppPlan plan;
    maker.makeBrokerRolePlan(plan);

    ASSERT_EQ(size_t(0), plan.packageInfos.size());
    ASSERT_EQ(size_t(4), plan.rolePlans.size());

    vector<string> roleNames = maker.getRoleName(RoleType::ROLE_TYPE_ALL);
    ASSERT_EQ(size_t(4), roleNames.size());
    for (size_t i = 0; i < roleNames.size(); i++) {
        innerTestForMakeAppPlan(roleNames[i], configPath, plan, maker._adminConfig->getExclusiveLevel());
    }
}

void AppPlanMakerTest::innerTestForMakeAppPlan(const string &roleName,
                                               const string &configPath,
                                               AppPlan &plan,
                                               BrokerExclusiveLevel el) {

    ASSERT_TRUE(plan.rolePlans.find(roleName) != plan.rolePlans.end());
    ASSERT_EQ(size_t(1), plan.rolePlans[roleName].slotResources.size());
    auto &firstResources = plan.rolePlans[roleName].slotResources[0];
    if (roleName.find("default") != string::npos) {
        EXPECT_EQ(int32_t(100), firstResources.resources[0].amount);
        EXPECT_EQ(string("cpu"), firstResources.resources[0].name);
        EXPECT_EQ(int32_t(1000), firstResources.resources[1].amount);
        EXPECT_EQ(string("mem"), firstResources.resources[1].name);
    } else {
        EXPECT_EQ(int32_t(10), firstResources.resources[0].amount);
        EXPECT_EQ(string("cpu"), firstResources.resources[0].name);
        EXPECT_EQ(int32_t(100), firstResources.resources[1].amount);
        EXPECT_EQ(string("mem"), firstResources.resources[1].name);
    }
    string cmdName = "swift_broker";
    if (el == BROKER_EL_ALL) {
        EXPECT_EQ(3, firstResources.resources.size());
        EXPECT_EQ(int32_t(1), firstResources.resources[2].amount);
        EXPECT_EQ(string("userName_serviceName_broker_11"), firstResources.resources[2].name);
        EXPECT_EQ(hippo::Resource::EXCLUSIVE, firstResources.resources[2].type);
    } else if (el == BROKER_EL_GROUP) {
        EXPECT_EQ(3, firstResources.resources.size());
        EXPECT_EQ(int32_t(1), firstResources.resources[2].amount);
        EXPECT_EQ(string("userName_serviceName_broker_default_11"), firstResources.resources[2].name);
        EXPECT_EQ(hippo::Resource::EXCLUSIVE, firstResources.resources[2].type);
    } else {
        EXPECT_EQ(2, firstResources.resources.size());
    }
    EXPECT_EQ(size_t(1), plan.rolePlans[roleName].processInfos.size());
    EXPECT_EQ(cmdName, plan.rolePlans[roleName].processInfos[0].cmd);
    EXPECT_EQ(cmdName, plan.rolePlans[roleName].processInfos[0].name);
    EXPECT_THAT(plan.rolePlans[roleName].processInfos[0].args,
                UnorderedElementsAre(PairType("-w", "${HIPPO_PROC_WORKDIR}"),
                                     PairType("-l", "/path/to/logconf"),
                                     PairType("-c", configPath),
                                     PairType("-r", roleName),
                                     PairType("-t", "16"),
                                     PairType("-i", "1"),
                                     PairType("-q", "200"),
                                     PairType("", "--recommendPort"),
                                     PairType("", "--enableANetMetric"),
                                     PairType("", "--enableARPCMetric")));
    EXPECT_EQ(size_t(0), plan.rolePlans[roleName].dataInfos.size());
    ASSERT_EQ(size_t(2), plan.rolePlans[roleName].packageInfos.size());
    EXPECT_EQ(PackageInfo::RPM, plan.rolePlans[roleName].packageInfos[0].type);
    EXPECT_EQ(string("swift_package"), plan.rolePlans[roleName].packageInfos[0].URI);
    EXPECT_EQ(PackageInfo::ARCHIVE, plan.rolePlans[roleName].packageInfos[1].type);
    EXPECT_EQ(string("other_packages"), plan.rolePlans[roleName].packageInfos[1].URI);
}

TEST_F(AppPlanMakerTest, testMakeAppPlanWithGroup) {
    string configPath = GET_TEST_DATA_PATH() + "app_plan_maker_test/normal_conf_with_group";
    AppPlanMaker maker;
    ASSERT_TRUE(maker.init(configPath, "11"));

    AppPlan plan;
    maker.makeBrokerRolePlan(plan);

    ASSERT_EQ(size_t(0), plan.packageInfos.size());
    ASSERT_EQ(size_t(6), plan.rolePlans.size());

    vector<string> roleNames = maker.getRoleName(RoleType::ROLE_TYPE_ALL);
    ASSERT_EQ(size_t(6), roleNames.size());
    for (size_t i = 0; i < roleNames.size(); i++) {
        innerTestForMakeAppPlan(roleNames[i], configPath, plan, maker._adminConfig->getExclusiveLevel());
    }
}

TEST_F(AppPlanMakerTest, testMakeAppPlanWithNetPriority) {
    string configPath = GET_TEST_DATA_PATH() + "app_plan_maker_test/normal_conf_with_net_priority";
    AppPlanMaker maker;
    ASSERT_TRUE(maker.init(configPath, "11"));

    AppPlan plan;
    maker.makeBrokerRolePlan(plan);

    ASSERT_EQ(size_t(0), plan.packageInfos.size());
    ASSERT_EQ(size_t(6), plan.rolePlans.size());

    auto iter = plan.rolePlans.begin();
    for (; iter != plan.rolePlans.end(); iter++) {
        const std::string &roleName = iter->first;
        string group, name;
        common::PathDefine::parseRoleGroup(roleName, group, name);
        auto &rolePlan = iter->second;
        if (group == "default") {
            ASSERT_EQ(size_t(1), rolePlan.containerConfigs.size());
            EXPECT_EQ("NET_PRIORITY=2", rolePlan.containerConfigs[0]);
        } else if (group == "test") {
            ASSERT_EQ(size_t(1), rolePlan.containerConfigs.size());
            EXPECT_EQ("NET_PRIORITY=5", rolePlan.containerConfigs[0]);
        }
    }
}

TEST_F(AppPlanMakerTest, testSetGroupNetPriority) {
    // test empty
    {
        std::map<std::string, uint32_t> groupNetPriorityMap;
        std::string groupName;
        RolePlan rolePlan;
        rolePlan.containerConfigs.push_back("abc");
        rolePlan.containerConfigs.push_back("NET_PRIORITY=1");
        AppPlanMaker maker;
        maker.setGroupNetPriority(groupNetPriorityMap, groupName, rolePlan);
        ASSERT_TRUE(rolePlan.containerConfigs.size() == 2);
        EXPECT_EQ("abc", rolePlan.containerConfigs[0]);
        EXPECT_EQ("NET_PRIORITY=1", rolePlan.containerConfigs[1]);
    }
    // test hasNetPriorityConfig
    {
        std::map<std::string, uint32_t> groupNetPriorityMap;
        groupNetPriorityMap["groupa"] = 3;
        RolePlan rolePlan;
        rolePlan.containerConfigs.push_back("abc");
        rolePlan.containerConfigs.push_back("NET_PRIORITY=1");
        AppPlanMaker maker;
        maker.setGroupNetPriority(groupNetPriorityMap, "groupa", rolePlan);
        ASSERT_TRUE(rolePlan.containerConfigs.size() == 2);
        EXPECT_EQ("abc", rolePlan.containerConfigs[0]);
        EXPECT_EQ("NET_PRIORITY=3", rolePlan.containerConfigs[1]);
    }
    // test !hasNetPriorityConfig
    {
        std::map<std::string, uint32_t> groupNetPriorityMap;
        groupNetPriorityMap["groupa"] = 3;
        RolePlan rolePlan;
        rolePlan.containerConfigs.push_back("abc");
        AppPlanMaker maker;
        maker.setGroupNetPriority(groupNetPriorityMap, "groupa", rolePlan);
        ASSERT_TRUE(rolePlan.containerConfigs.size() == 2);
        EXPECT_EQ("abc", rolePlan.containerConfigs[0]);
        EXPECT_EQ("NET_PRIORITY=3", rolePlan.containerConfigs[1]);
    }
}

TEST_F(AppPlanMakerTest, testGetRoleNames) {
    AppPlanMaker maker;
    maker._brokerRoleNames.emplace_back("broker_1_1");
    maker._brokerRoleNames.emplace_back("broker_3_3");

    vector<string> roles;
    roles = maker.getRoleName(ROLE_TYPE_BROKER);
    EXPECT_EQ(2, roles.size());
    EXPECT_EQ("broker_1_1", roles[0]);
    EXPECT_EQ("broker_3_3", roles[1]);

    roles = maker.getRoleName(ROLE_TYPE_ALL);
    EXPECT_EQ(2, roles.size());
    EXPECT_EQ("broker_1_1", roles[0]);
    EXPECT_EQ("broker_3_3", roles[1]);
}

}; // namespace admin
}; // namespace swift
