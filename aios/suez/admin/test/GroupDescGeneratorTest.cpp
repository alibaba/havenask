#include "suez/admin/GroupDescGenerator.h"

#include <ext/alloc_traits.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "TargetLoader.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/fast_jsonizable_dec.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "carbon/legacy/RoleManagerWrapperInternal.h"
#include "carbon/proto/Carbon.pb.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "hippo/proto/Common.pb.h"
#include "suez/sdk/JsonNodeRef.h"
#include "unittest/unittest.h"

namespace suez {
class GroupDescGeneratorTest_testGenerateGroupDescs_ServiceNotMatch_Test;
class GroupDescGeneratorTest_testGenerateGroupDescs_SpecifyRoleCount_Test;
class GroupDescGeneratorTest_testGenerateGroupDescs_WithServiceInfoClosed_Test;
class GroupDescGeneratorTest_testGenerateGroupDescs_WithServiceInfoOpen_Test;
class GroupDescGeneratorTest_testGenerateGroupDescs_WithoutTableInfo_Test;
class GroupDescGeneratorTest_testGenerateGroupDescs_customsignature2_Test;
class GroupDescGeneratorTest_testGenerateGroupDescs_customsignature_Test;
} // namespace suez

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace http_arpc;
using namespace carbon;
using namespace carbon::proto;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, GroupDescGeneratorTest);

class GroupDescGeneratorTest : public TESTBASE {
protected:
    std::string DATA_PATH(const std::string &fileName) { return GET_TEST_DATA_PATH() + "admin_test/" + fileName; }
};

TEST_F(GroupDescGeneratorTest, testGetRoleCount) {
    string zoneStr = "{}";
    JsonMap zone;
    FromJsonString(zone, zoneStr);
    EXPECT_EQ(1, GroupDescGenerator::getRoleCount(&zone));

    zoneStr = R"({"role_count":0})";
    FromJsonString(zone, zoneStr);
    EXPECT_EQ(1, GroupDescGenerator::getRoleCount(&zone));

    zoneStr = R"({"role_count":2})";
    FromJsonString(zone, zoneStr);
    EXPECT_EQ(2, GroupDescGenerator::getRoleCount(&zone));

    zoneStr = R"({"role_count":-1})";
    FromJsonString(zone, zoneStr);
    EXPECT_THROW(GroupDescGenerator::getRoleCount(&zone), ExceptionBase);

    zoneStr = R"({})";
    FromJsonString(zone, zoneStr);
    EXPECT_EQ(1, GroupDescGenerator::getRoleCount(&zone));

    zoneStr = R"({"table_info":123})";
    FromJsonString(zone, zoneStr);
    EXPECT_THROW(GroupDescGenerator::getRoleCount(&zone), ExceptionBase);

    zoneStr = R"({"table_info":{"table1":123}})";
    FromJsonString(zone, zoneStr);
    EXPECT_THROW(GroupDescGenerator::getRoleCount(&zone), ExceptionBase);

    zoneStr = R"({"table_info":{"table1":{"g1":12}}})";
    FromJsonString(zone, zoneStr);
    EXPECT_THROW(GroupDescGenerator::getRoleCount(&zone), ExceptionBase);

    zoneStr = R"({"table_info":{"table1":{"g1":{}}}})";
    FromJsonString(zone, zoneStr);
    EXPECT_THROW(GroupDescGenerator::getRoleCount(&zone), ExceptionBase);

    zoneStr = R"({"table_info":{"table1":{"g1":{"partitions":123}}}})";
    FromJsonString(zone, zoneStr);
    EXPECT_THROW(GroupDescGenerator::getRoleCount(&zone), ExceptionBase);

    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("zone_valid_role_count.json"), zone));
    EXPECT_EQ(2, GroupDescGenerator::getRoleCount(&zone));

    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("zone_invalid_role_count.json"), zone));
    EXPECT_THROW(GroupDescGenerator::getRoleCount(&zone), ExceptionBase);

    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("zone_no_role_count.json"), zone));
    EXPECT_EQ(3, GroupDescGenerator::getRoleCount(&zone));
}

TEST_F(GroupDescGeneratorTest, testGenerateServiceDesc) {
    string hippoConfStr;
    JsonMap hippoConf;
    carbon::GroupDesc groupDesc;
    RoleDescription roleDesc;
    roleDesc.set_rolename("role1");
    groupDesc.roles.push_back(roleDesc);

    hippoConfStr = R"({})";
    FromJsonString(hippoConf, hippoConfStr);
    GroupDescGenerator::generateServiceDesc(&hippoConf, groupDesc);
    EXPECT_EQ(0u, groupDesc.services.size());

    hippoConfStr = R"({"service_desc":{}})";
    FromJsonString(hippoConf, hippoConfStr);
    GroupDescGenerator::generateServiceDesc(&hippoConf, groupDesc);
    EXPECT_EQ(0u, groupDesc.services.size());

    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("service_desc_valid.json"), hippoConf));
    GroupDescGenerator::generateServiceDesc(&hippoConf, groupDesc);
    ASSERT_EQ(1u, groupDesc.services.size());
    string publishServiceStr = R"({
"roleName":
  "role1",
"serviceConfigs":
  [
    {
    "configStr":
      "{\n\"domain\":\n  \"com.taobao.vip_test\",\n\"jmenvDom\":\n  \"172.21.95.147:8080\",\n\"port\":\n  9001,\n\"token\":\n  \"123456\"\n}",
    "name":
      "vip",
    "statusPath":
      "..\/suez_worker\/SearchService\/vip_status",
    "type":
      "ST_VIP"
    },
    {
    "configStr":
      "{\n\"cm2_server_cluster_name\":\n  \"sp_wireless_http2\",\n\"cm2_server_leader_path\":\n  \"\\\/cm_server_test\",\n\"cm2_server_zookeeper_host\":\n  \"10.101.83.215:2181,10.101.83.226:2181,10.101.83.227:2181\",\n\"httpPort\":\n  20009,\n\"idc_type\":\n  1,\n\"signature\":\n  \"sig2\",\n\"tcpPort\":\n  21009,\n\"weight\":\n  100\n}",
    "metaStr":
      "{\n\"topostr\":\n  \"top1\",\n\"weight\":\n  8\n}",
    "name":
      "cm2",
    "statusPath":
      "..\/suez_worker\/SearchService\/cm2_status",
    "type":
      "ST_CM2"
    }
  ]
})";
    EXPECT_EQ(publishServiceStr, ProtoJsonizer::toJsonString(groupDesc.services[0]));
    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("service_desc_invalid.json"), hippoConf));
    EXPECT_THROW(GroupDescGenerator::generateServiceDesc(&hippoConf, groupDesc), ExceptionBase);

    groupDesc.services.clear();
    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("service_desc_test_metastr.json"), hippoConf));
    GroupDescGenerator::generateServiceDesc(&hippoConf, groupDesc);
    ASSERT_EQ(1u, groupDesc.services.size());
    publishServiceStr = R"({
"roleName":
  "role1",
"serviceConfigs":
  [
    {
    "configStr":
      "{\n\"cm2_server_cluster_name\":\n  \"sp_wireless_http2\",\n\"cm2_server_leader_path\":\n  \"\\\/cm_server_test\",\n\"cm2_server_zookeeper_host\":\n  \"10.101.83.215:2181,10.101.83.226:2181,10.101.83.227:2181\",\n\"httpPort\":\n  20009,\n\"idc_type\":\n  1,\n\"signature\":\n  \"sig2\",\n\"tcpPort\":\n  21009,\n\"weight\":\n  100\n}",
    "metaStr":
      "12",
    "name":
      "cm2",
    "statusPath":
      "..\/suez_worker\/SearchService\/cm2_status",
    "type":
      "ST_CM2"
    }
  ]
})";
    EXPECT_EQ(publishServiceStr, ProtoJsonizer::toJsonString(groupDesc.services[0]));
}

TEST_F(GroupDescGeneratorTest, testGenServiceInfoZoneName) {
    string zoneName = "abcd";
    string expect = zoneName;
    EXPECT_EQ(expect, GroupDescGenerator::genServiceInfoZoneName(zoneName));
    zoneName = "abcd -abcd";
    expect = zoneName;
    EXPECT_EQ(expect, GroupDescGenerator::genServiceInfoZoneName(zoneName));
    zoneName = "abcd-abcd -abcd";
    expect = "abcd";
    EXPECT_EQ(expect, GroupDescGenerator::genServiceInfoZoneName(zoneName));
    zoneName = "abcd-abcd - abcd";
    expect = " abcd";
    EXPECT_EQ(expect, GroupDescGenerator::genServiceInfoZoneName(zoneName));
}

TEST_F(GroupDescGeneratorTest, testGenerateRoleDesc) {
    {
        // processinfo empty
        JsonMap jsonMap = {{"role_desc", JsonMap()}};
        GroupDesc groupDesc;
        EXPECT_THROW(GroupDescGenerator::generateRoleDesc(2, "zone1", &jsonMap, "", ""), ExceptionBase);
    }

    JsonMap jsonMap;
    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("hippoInfo.json"), jsonMap));
    {
        auto roleDesc = GroupDescGenerator::generateRoleDesc(2, "zone1", &jsonMap, "", "");
        EXPECT_EQ("zone1_role_partition_2", roleDesc.rolename());
        int argsCount = roleDesc.processinfos(0).args_size();
        auto arg1 = roleDesc.processinfos(0).args(argsCount - 3);
        EXPECT_EQ("--env", arg1.key());
        EXPECT_EQ("roleName=zone1_role_partition_2", arg1.value());
        auto arg2 = roleDesc.processinfos(0).args(argsCount - 2);
        EXPECT_EQ("--env", arg2.key());
        EXPECT_EQ("zoneName=zone1", arg2.value());
        auto arg3 = roleDesc.processinfos(0).args(argsCount - 1);
        EXPECT_EQ("--env", arg3.key());
        EXPECT_EQ("partId=2", arg3.value());

        int envsCount = roleDesc.processinfos(0).envs_size();
        auto env1 = roleDesc.processinfos(0).envs(envsCount - 1);
        EXPECT_EQ("RS_ALLOW_RELOAD_BY_CONFIG", env1.key());
        EXPECT_EQ("true", env1.value());
    }
    {
        AnyCast<JsonMap>(&jsonMap["role_desc"])->erase("roleName");
        auto roleDesc = GroupDescGenerator::generateRoleDesc(2, "zone1", &jsonMap, "", "");
        EXPECT_EQ("zone1_partition_2", roleDesc.rolename());
        int argsCount = roleDesc.processinfos(0).args_size();
        auto arg1 = roleDesc.processinfos(0).args(argsCount - 3);
        EXPECT_EQ("--env", arg1.key());
        EXPECT_EQ("roleName=zone1_partition_2", arg1.value());

        int envsCount = roleDesc.processinfos(0).envs_size();
        auto env1 = roleDesc.processinfos(0).envs(envsCount - 1);
        EXPECT_EQ("RS_ALLOW_RELOAD_BY_CONFIG", env1.key());
        EXPECT_EQ("true", env1.value());
    }
}

TEST_F(GroupDescGeneratorTest, testGenerateTarget) {
    JsonMap jsonMap;
    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("test_generate_target.json"), jsonMap));
    JsonNodeRef ref(jsonMap);
    JsonMap *srcZoneConf = AnyCast<JsonMap>(ref.read("zones.zone1"));
    ASSERT_TRUE(srcZoneConf);
    {
        JsonMap zoneConf = JsonNodeRef::copy(*srcZoneConf);
        RoleDescription roleDesc;
        roleDesc.set_rolename("role1");
        GroupDescGenerator::generateTarget("zone", roleDesc, 0, 3, &zoneConf, nullptr);
        JsonNodeRef tmpRef(zoneConf);
        string nodePath;
        EXPECT_FALSE(tmpRef.read("table_info.unknown"));
        EXPECT_TRUE(tmpRef.read("table_info.table1.g1.partitions.0_65535"));
        EXPECT_TRUE(tmpRef.read("table_info.table1.g2.partitions.0_65535"));
        EXPECT_TRUE(tmpRef.read("table_info.table2.g1.partitions.0_32767"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g1.partitions.32768_65535"));
        EXPECT_TRUE(tmpRef.read("table_info.table2.g2.partitions.0_16383"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g2.partitions.16384_32767"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g2.partitions.32768_49151"));
        EXPECT_TRUE(tmpRef.read("table_info.table2.g2.partitions.49152_65535"));
        EXPECT_TRUE(tmpRef.read("table_info.table3.g1.partitions.0_65535"));
        Any *tmpAny4 = tmpRef.read("service_info");
        EXPECT_TRUE(tmpAny4 != NULL);
        string jsonStr1;
        ToString(*tmpAny4, jsonStr1, true);
        EXPECT_EQ("{\"part_count\":3,\"part_id\":0,\"role_name\":\"role1\",\"zone_name\":\"zone\"}", jsonStr1);
    }
    {
        JsonMap zoneConf = JsonNodeRef::copy(*srcZoneConf);
        RoleDescription roleDesc;
        roleDesc.set_rolename("role1");
        GroupDescGenerator::generateTarget("zone", roleDesc, 1, 3, &zoneConf, nullptr);
        string targetStr;
        ToString(zoneConf, targetStr, true);
        JsonNodeRef tmpRef(zoneConf);
        EXPECT_FALSE(tmpRef.read("table_info.unknown"));
        EXPECT_TRUE(tmpRef.read("table_info.table1.g1.partitions.0_65535"));
        EXPECT_FALSE(tmpRef.read("table_info.table1.g2"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g1.partitions.0_32767"));
        EXPECT_TRUE(tmpRef.read("table_info.table2.g1.partitions.32768_65535"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g2.partitions.0_16383"));
        EXPECT_TRUE(tmpRef.read("table_info.table2.g2.partitions.16384_32767"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g2.partitions.32768_49151"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g2.partitions.49152_65535"));
        EXPECT_TRUE(tmpRef.read("table_info.table3"));
        EXPECT_TRUE(tmpRef.read("table_info.table3.g1"));
        EXPECT_TRUE(tmpRef.read("table_info.table3.g1.partitions.0_65535"));
        Any *tmpAny4 = tmpRef.read("service_info");
        EXPECT_TRUE(tmpAny4 != NULL);
        string jsonStr1;
        ToString(*tmpAny4, jsonStr1, true);
        EXPECT_EQ("{\"part_count\":3,\"part_id\":1,\"role_name\":\"role1\",\"zone_name\":\"zone\"}", jsonStr1);
    }
    {
        JsonMap zoneConf = JsonNodeRef::copy(*srcZoneConf);
        RoleDescription roleDesc;
        roleDesc.set_rolename("role1");
        GroupDescGenerator::generateTarget("zone", roleDesc, 2, 3, &zoneConf, nullptr);
        JsonNodeRef tmpRef(zoneConf);
        EXPECT_FALSE(tmpRef.read("table_info.unknown"));
        EXPECT_TRUE(tmpRef.read("table_info.table1.g1.partitions.0_65535"));
        EXPECT_FALSE(tmpRef.read("table_info.table1.g2"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g1"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g2.partitions.0_16383"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g2.partitions.16384_32767"));
        EXPECT_TRUE(tmpRef.read("table_info.table2.g2.partitions.32768_49151"));
        EXPECT_FALSE(tmpRef.read("table_info.table2.g2.partitions.49152_65535"));
        EXPECT_TRUE(tmpRef.read("table_info.table3"));
        EXPECT_TRUE(tmpRef.read("table_info.table3.g1"));
        EXPECT_TRUE(tmpRef.read("table_info.table3.g1.partitions.0_65535"));
        Any *tmpAny4 = tmpRef.read("service_info");
        EXPECT_TRUE(tmpAny4 != NULL);
        string jsonStr1;
        ToString(*tmpAny4, jsonStr1, true);
        EXPECT_EQ("{\"part_count\":3,\"part_id\":2,\"role_name\":\"role1\",\"zone_name\":\"zone\"}", jsonStr1);
    }
}

TEST_F(GroupDescGeneratorTest, testGeneratePartitionTarget) {
    {
        JsonMap genInfo;
        ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("genInfo.json"), genInfo));
        GroupDescGenerator::generatePartitionTarget(0, 100, &genInfo);

        JsonNodeRef ref(genInfo);
        Any *node1Any = ref.read("partitions.4_36000");
        EXPECT_TRUE(node1Any != NULL);
        Any *node2Any = ref.read("partitions.36001_40000");
        EXPECT_TRUE(node2Any == NULL);
    }
    {
        JsonMap genInfo;
        ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("genInfo.json"), genInfo));
        GroupDescGenerator::generatePartitionTarget(1, 100, &genInfo);

        JsonNodeRef ref1(genInfo);
        Any *node3Any = ref1.read("partitions.4_36000");
        EXPECT_TRUE(node3Any == NULL);
        Any *node4Any = ref1.read("partitions.36001_40000");
        EXPECT_TRUE(node4Any != NULL);
    }
    {
        JsonMap genInfo;
        ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("genInfo.json"), genInfo));
        GroupDescGenerator::generatePartitionTarget(2, 100, &genInfo);

        JsonNodeRef ref1(genInfo);
        Any *node3Any = ref1.read("partitions.4_36000");
        EXPECT_TRUE(node3Any == NULL);
        Any *node4Any = ref1.read("partitions.36001_40000");
        EXPECT_TRUE(node4Any == NULL);
    }
    {
        JsonMap genInfo;
        ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("genInfo.json"), genInfo));
        genInfo["distribute_mode"] = string("unknown");
        EXPECT_THROW(GroupDescGenerator::generatePartitionTarget(2, 100, &genInfo), ExceptionBase);
    }
}

#define GEN_GROUP_DESCS(data_path)                                                                                     \
    JsonMap jsonMap;                                                                                                   \
    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH(data_path), jsonMap));                                            \
    map<string, GroupDesc> groups;                                                                                     \
    GroupDescGenerator::generateGroupDescs(jsonMap, groups);

string removeSwitchMode(const RoleDescription &role) {
    RoleDescription tmpRole = role;
    JsonMap customInfo;
    FromJsonString(customInfo, role.custominfo());
    customInfo.erase("switch_mode");
    tmpRole.set_custominfo(ToJsonString(customInfo, true));
    return ProtoJsonizer::toJsonString(tmpRole);
}

TEST_F(GroupDescGeneratorTest, testGenerateGroupDescs) {
    map<string, GroupDesc> groupsByRow, groupsByColumn;
    JsonMap jsonMap, result1, result2;
    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("test_generate_group_desc.json"), jsonMap));

    GroupDescGenerator::generateGroupDescs(jsonMap, groupsByRow);
    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("test_generate_group_desc_byrow_result.json"), result1));
    EXPECT_EQ(ToJsonString(result1), ToJsonString(groupsByRow));

    vector<string> groupName;
    EXPECT_TRUE(GroupDescGenerator::getGroupNameForSystemInfo(jsonMap, "zone1", groupName));
    EXPECT_THAT(groupName, ElementsAre("zone1"));

    JsonNodeRef ref(jsonMap);
    ASSERT_TRUE(ref.create("zones.zone1.switch_mode", string("column")));
    GroupDescGenerator::generateGroupDescs(jsonMap, groupsByColumn);
    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH("test_generate_group_desc_bypart_result.json"), result2));
    EXPECT_EQ(ToJsonString(result2), ToJsonString(groupsByColumn));

    EXPECT_EQ(ToJsonString(groupsByRow["zone2"]), ToJsonString(groupsByColumn["zone2"]));
    EXPECT_EQ(ProtoJsonizer::toJsonString(groupsByRow["zone1"].roles[0]),
              removeSwitchMode(groupsByColumn["zone1.0"].roles[0]));
    EXPECT_EQ(ProtoJsonizer::toJsonString(groupsByRow["zone1"].roles[1]),
              removeSwitchMode(groupsByColumn["zone1.1"].roles[0]));

    EXPECT_EQ(ProtoJsonizer::toJsonString(groupsByRow["zone1"].services[0]),
              ProtoJsonizer::toJsonString(groupsByColumn["zone1.0"].services[0]));
    EXPECT_EQ(ProtoJsonizer::toJsonString(groupsByRow["zone1"].services[1]),
              ProtoJsonizer::toJsonString(groupsByColumn["zone1.1"].services[0]));

    groupName.clear();
    EXPECT_TRUE(GroupDescGenerator::getGroupNameForSystemInfo(jsonMap, "zone1", groupName));
    EXPECT_THAT(groupName, ElementsAre("zone1.0", "zone1.1"));

    groupName.clear();
    EXPECT_FALSE(GroupDescGenerator::getGroupNameForSystemInfo(jsonMap, "not_exist", groupName));
    EXPECT_THAT(groupName, ElementsAre());

    EXPECT_FALSE(GroupDescGenerator::getGroupNameForSystemInfo(JsonMap(), "not_exist", groupName));
    EXPECT_THAT(groupName, ElementsAre());
}

TEST_F(GroupDescGeneratorTest, testGenerateGroupDescs_SpecifyRoleCount) {
    {
        GEN_GROUP_DESCS("test_specify_role_count.json");
        EXPECT_EQ(size_t(3), groups["zone1"].roles.size());
    }
    {
        GEN_GROUP_DESCS("test_specify_role_count_zero.json");
        EXPECT_EQ(size_t(1), groups["zone1"].roles.size());
    }
}

TEST_F(GroupDescGeneratorTest, testGenerateGroupDescs_WithServiceInfoClosed) {
    GEN_GROUP_DESCS("target_with_service_info_closed.json");
    EXPECT_EQ(size_t(1), groups.size());
    auto group1 = groups["zone1"];
    EXPECT_EQ("biz1.\"config_path\" biz2.\"config_path\"", group1.roles[0].signature());
    EXPECT_EQ("{\"biz_info\":{\"biz1\":{\"config_path\":\"config_path\"},\"biz2\":{\"config_path\":\"config_path\"}},"
              "\"disk_size\":2147483647,\"service_info\":{\"part_count\":1,\"part_id\":0,\"role_name\":\"zone1_role_"
              "partition_0\",\"sub_cm2_configs\":[{\"cm2_server_cluster_name\":\"turing_seq_service_internal\",\"cm2_"
              "server_leader_path\":\"\\/suez_ops\\/"
              "cm_server_ha3\",\"cm2_server_zookeeper_host\":\"search-zk-cm2-ha3-ea120.vip.tbsite.net:2187\"}],\"zone_"
              "name\":\"zone1\"},\"table_info\":{}}",
              group1.roles[0].custominfo());
}

TEST_F(GroupDescGeneratorTest, testGenerateGroupDescs_WithServiceInfoOpen) {
    GEN_GROUP_DESCS("target_with_service_info_open.json");
    EXPECT_EQ(size_t(1), groups.size());
    auto group1 = groups["zone1"];
    EXPECT_EQ("biz1.\"config_path\" biz2.\"config_path\" [\n  {\n  \"cm2_server_cluster_name\":\n    "
              "\"turing_seq_service_internal\",\n  \"cm2_server_leader_path\":\n    \"\\/suez_ops\\/cm_server_ha3\",\n "
              " \"cm2_server_zookeeper_host\":\n    \"search-zk-cm2-ha3-ea120.vip.tbsite.net:2187\"\n  }\n]",
              group1.roles[0].signature());
    EXPECT_EQ("{\"biz_info\":{\"biz1\":{\"config_path\":\"config_path\"},\"biz2\":{\"config_path\":\"config_path\"}},"
              "\"disk_size\":2147483647,\"service_info\":{\"part_count\":1,\"part_id\":0,\"role_name\":\"zone1_role_"
              "partition_0\",\"sub_cm2_configs\":[{\"cm2_server_cluster_name\":\"turing_seq_service_internal\",\"cm2_"
              "server_leader_path\":\"\\/suez_ops\\/"
              "cm_server_ha3\",\"cm2_server_zookeeper_host\":\"search-zk-cm2-ha3-ea120.vip.tbsite.net:2187\"}],\"zone_"
              "name\":\"zone1\"},\"service_info_need_rolling\":true,\"table_info\":{}}",
              group1.roles[0].custominfo());
}

TEST_F(GroupDescGeneratorTest, testGenerateGroupDescs_WithoutTableInfo) {
    GEN_GROUP_DESCS("target_without_tableinfo.json");
    EXPECT_EQ(size_t(1), groups.size());
    auto group1 = groups["zone1"];
    EXPECT_EQ("biz1.\"config_path\" biz2.\"config_path\"", group1.roles[0].signature());
    EXPECT_EQ("{\"biz_info\":{\"biz1\":{\"config_path\":\"config_path\"},\"biz2\":{\"config_path\":\"config_path\"}},"
              "\"disk_size\":2147483647,\"service_info\":{\"part_count\":1,\"part_id\":0,\"role_name\":\"zone1_role_"
              "partition_0\",\"zone_name\":\"zone1\"},\"table_info\":{}}",
              group1.roles[0].custominfo());
}

TEST_F(GroupDescGeneratorTest, testGenerateGroupDescs_ServiceNotMatch) {
    GEN_GROUP_DESCS("targetMulti_error.json");
    EXPECT_EQ(1u, groups.size());
}

TEST_F(GroupDescGeneratorTest, testGenerateGroupDescs_customsignature) {
    GEN_GROUP_DESCS("test_generate_custom_signature.json");
    ASSERT_EQ(1u, groups.size());
    EXPECT_EQ("table1.g1.0_65535 table1.g2.0_65535 table2.g1.0_32767 table2.g2.0_16383 table3.g1.0_65535 biz1 biz2 "
              "table1.g1.\"config_path\" table1.g2.\"config_path\" table2.g1.\"config_path\" table2.g2.\"config_path\" "
              "table3.g1.\"config_path\" "
              "table1.g2.0_65535.100",
              groups["zone1"].roles[0].signature());
    EXPECT_EQ("table1.g1.0_65535 table2.g1.32768_65535 table2.g2.16384_32767 table3.g1.0_65535 biz1 biz2 "
              "table1.g1.\"config_path\" table2.g1.\"config_path\" table2.g2.\"config_path\" table3.g1.\"config_path\"",
              groups["zone1"].roles[1].signature());
}

TEST_F(GroupDescGeneratorTest, testGenerateGroupDescs_customsignature2) {
    GEN_GROUP_DESCS("test_generate_custom_signature2.json");
    ASSERT_EQ(1u, groups.size());
    EXPECT_EQ("table1.g1.0_65535.10 table1.g2.0_65535.12 table2.g1.0_32767.10 table2.g2.0_16383.10 "
              "table3.g1.0_65535.10 biz1 biz2 \"app_config_path\" {\n\"a\":\n  1,\n\"b\":\n  2\n}",
              groups["zone1"].roles[0].signature());
    EXPECT_EQ("table1.g1.0_65535.10 table2.g1.32768_65535.10 table2.g2.16384_32767.10 table3.g1.0_65535.10 biz1 biz2 "
              "\"app_config_path\" {\n\"a\":\n  1,\n\"b\":\n  2\n}",
              groups["zone1"].roles[1].signature());
}

TEST_F(GroupDescGeneratorTest, testResourceHeterogeneous) {
    GEN_GROUP_DESCS("test_generate_group_desc.json");
    {
        auto &group1 = groups["zone1"];
        const auto &role0 = group1.roles[0];
        EXPECT_EQ(1u, role0.resources_size());
        const auto &role0Resource = role0.resources(0);
        EXPECT_EQ(1u, role0Resource.resources_size());
        const auto &cpu = role0Resource.resources(0);
        EXPECT_EQ(2u, cpu.amount());

        const auto &role1 = group1.roles[1];
        EXPECT_EQ(1u, role1.resources_size());
        const auto &role1Resource = role1.resources(0);
        EXPECT_EQ(1u, role1Resource.resources_size());
        const auto &cpu1 = role1Resource.resources(0);
        EXPECT_EQ(3u, cpu1.amount());
    }
    {
        auto &group2 = groups["zone2"];
        const auto &role1 = group2.roles[1];
        EXPECT_EQ(1u, role1.resources_size());
        const auto &role1Resource = role1.resources(0);
        EXPECT_EQ(1u, role1Resource.resources_size());
        const auto &cpu1 = role1Resource.resources(0);
        EXPECT_EQ(4u, cpu1.amount());

        const auto &role0 = group2.roles[0];
        EXPECT_EQ(1u, role0.resources_size());
        const auto &role0Resource = role0.resources(0);
        EXPECT_EQ(1u, role0Resource.resources_size());
        const auto &cpu0 = role0Resource.resources(0);
        EXPECT_EQ(4u, cpu0.amount());
    }
}

TEST_F(GroupDescGeneratorTest, extractDiskSizeQuota) {
    carbon::proto::RoleDescription roleDesc;
    auto resources = roleDesc.add_resources();
    resources->add_resources()->set_name("cpu");
    resources->add_resources()->set_name("mem");

    EXPECT_EQ(numeric_limits<int32_t>::max(), GroupDescGenerator::extractDiskSizeQuota(roleDesc));
    auto resource = resources->add_resources();
    resource->set_name("disk_size");
    resource->set_amount(1000);
    EXPECT_EQ(1000, GroupDescGenerator::extractDiskSizeQuota(roleDesc));
    resource->set_name("disk_size_9999");
    resource->set_amount(256);
    EXPECT_EQ(256, GroupDescGenerator::extractDiskSizeQuota(roleDesc));
}

TEST_F(GroupDescGeneratorTest, testGenerateGroupDescs_LocalCm2Mode) {
    JsonMap jsonMap, statusInfo;
    const std::string data_path = "test_generate_group_desc_extra_role_config.json";
    const std::string status_info_path = "status_info.json";
    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH(data_path), jsonMap));
    ASSERT_TRUE(TargetLoader::loadFromFile(DATA_PATH(status_info_path), statusInfo));
    map<string, GroupDesc> groups;
    GroupDescGenerator::generateGroupDescs(jsonMap, &statusInfo, groups);
    ASSERT_EQ(2u, groups.size());

    string customInfoStr = groups["zone1"].roles[1].custominfo();
    JsonMap customInfo;
    FromJsonString(customInfo, customInfoStr);
    auto serviceInfo = getField(&customInfo, "service_info", JsonMap());
    auto cm2Config = getField(&serviceInfo, "cm2_config", JsonMap());
    auto local = getField(&cm2Config, "local", JsonArray());

    ASSERT_EQ(7u, local.size());
    vector<string> bizNameVec{
        "default", "default_agg", "default_sql", "para_search_16", "para_search_2", "para_search_4", "para_search_8"};
    for (size_t i = 0; i < 7; ++i) {
        auto node = AnyCast<JsonNodeRef::JsonMap>(local[i]);
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

// // TODO without tableinfo

// TEST_F(GroupDescGeneratorTest, testGenerateBigGroupDescs) {
//     GroupDescGenerator serviceImpl;
//     string testPath = GET_TEST_DATA_PATH() + "admin_test/";
//     string testFile = testPath + "big.json";
//     JsonMap jsonMap;
//     ASSERT_TRUE(TargetLoader::loadFromFile(testFile, jsonMap));
//     JsonNodeRef ref(jsonMap);
//     map<string, GroupDesc> groupDescs;
//     int64_t currentTime = autil::TimeUtility::currentTime();
//     ASSERT_TRUE(serviceImpl.generateGroupDescs(jsonMap, groupDescs));
//     int64_t endTime = autil::TimeUtility::currentTime();
//     cout << "use " << (double)(endTime - currentTime) / 1000.0 << "ms" << endl;
//     Any any1 = string("a");
//     Any any2 = string("b");
//     Any any3 = any2;
//     any2 = any1;
//     cout << "anys" << endl;
//     cout << AnyCast<string>(any1) << endl;
//     cout << AnyCast<string>(any2) << endl;
//     cout << AnyCast<string>(any3) << endl;
// }

// TEST_F(GroupDescGeneratorTest, testCopyMap) {
//     JsonMap map1;
//     JsonMap innerMap1;
//     innerMap1["a"] = 1;
//     innerMap1["b"] = 2;
//     map1["inner"] = innerMap1;
//     JsonMap map2;
//     map2 = map1;
//     AnyCast<JsonMap>(&map2["inner"])->erase("a");
//     cout << ToJsonString(map1) << endl;
// }

} // namespace suez
