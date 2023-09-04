#include "suez/admin/AdminTarget.h"

#include "TargetLoader.h"
#include "unittest/unittest.h"

namespace suez {

class AdminTargetTest : public TESTBASE {};

TEST_F(AdminTargetTest, testHippoConfig) {
    AdminTarget target;
    auto zoneA = ZoneTarget();
    auto zoneB = ZoneTarget();
    zoneA.hippoConfig = "{}";
    zoneB.hippoConfig = "{\n\"role\":\"rolexxx\"\n}";
    target.zones["a"] = zoneA;
    target.zones["b"] = zoneB;
    JsonNodeRef::JsonMap jsonMap = target.toJsonMap();
    auto targetStr = FastToJsonString(jsonMap);
    std::string expectTargetStr =
        "{\"zones\":{\"a\":{\"biz_info\":{},\"group_name\":\"\",\"hippo_config\":{},\"role_count\":0,\"service_info\":{"
        "\"cm2_config\":{},\"part_count\":1,\"part_id\":0,\"role_name\":\"\",\"sub_cm2_configs\":[],\"zone_name\":\"\"}"
        ",\"table_info\":{}},\"b\":{\"biz_info\":{},\"group_name\":\"\",\"hippo_config\":{\"role\":\"rolexxx\"},\"role_"
        "count\":0,\"service_info\":{\"cm2_config\":{},\"part_count\":1,\"part_id\":0,\"role_name\":\"\",\"sub_cm2_"
        "configs\":[],\"zone_name\":\"\"},\"table_info\":{}}}}";
    EXPECT_EQ(expectTargetStr, targetStr);
}

// TEST_F(AdminTargetTest, testSimple) {
//     JsonNodeRef::JsonMap jsonMap;
//     ASSERT_TRUE(TargetLoader::loadFromFile(GET_TEST_DATA_PATH() + "/admin_test/target_to_catalog.json", jsonMap));

//     AdminTarget target;
//     ASSERT_TRUE(target.parseFrom(jsonMap));

//     ASSERT_EQ(2, target.zones.size());
//     auto it = target.zones.find("pub_na63_remote_table_test-na63_catalog-zone1");
//     ASSERT_TRUE(it != target.zones.end());
//     auto &zoneInfo = it->second;
//     ASSERT_EQ(1, zoneInfo.bizInfos.size());
//     ASSERT_EQ(1, zoneInfo.bizInfos.count("biz1"));
//     ASSERT_EQ(1, zoneInfo.tableInfos.size());
//     ASSERT_EQ(1, zoneInfo.tableInfos.count("table1"));
//     ASSERT_EQ(
//         "{\n\"proxy\":\n  {\n  \"configStr\":\n    {\n    \"domain\":\n      \"com.taobao.vip_test\",\n    "
//         "\"jmenvDom\":\n      \"172.21.95.147:8080\",\n    \"port\":\n      9001,\n    \"token\":\n      \"123456\"\n
//         " "  },\n  \"name\":\n    \"vip\",\n  \"statusPath\":\n    \"..\\/role_proc\\/vip.status\",\n  \"type\":\n "
//         "\"ST_VIP\"\n  },\n\"searcher\":\n  {\n  \"configStr\":\n    {\n    \"cm2_server_cluster_name\":\n      "
//         "\"sp_wireless_http2\",\n    \"cm2_server_leader_path\":\n      \"\\/cm_server_test\",\n    "
//         "\"cm2_server_zookeeper_host\":\n      \"10.101.83.215:2181,10.101.83.226:2181,10.101.83.227:2181\",\n    "
//         "\"httpPort\":\n      20009,\n    \"idc_type\":\n      1,\n    \"signature\":\n      \"sig2\",\n    "
//         "\"tcpPort\":\n      21009,\n    \"weight\":\n      100\n    },\n  \"name\":\n    \"cm2\",\n  "
//         "\"statusPath\":\n    \"..\\/role_proc\\/cm2.status\",\n  \"type\":\n    \"ST_CM2\"\n  }\n}",
//         zoneInfo.serviceDesc);

//     auto &table1 = zoneInfo.tableInfos["table1"];
//     ASSERT_EQ(2, table1.size());

//     ASSERT_EQ(1, table1.count("1658131669"));
//     auto &g1 = table1["1658131669"];
//     ASSERT_EQ(1658131669, g1.generationId);
//     ASSERT_EQ("config_path", g1.configPath);
//     ASSERT_EQ("/path/to/table1/1", g1.indexRoot);
//     ASSERT_EQ(1, g1.partitions.size());
//     ASSERT_EQ(1, g1.partitions.count("0_65535"));
//     auto &p1 = g1.partitions["0_65535"];
//     ASSERT_EQ(0, p1.rangeFrom);
//     ASSERT_EQ(65535, p1.rangeTo);

//     ASSERT_EQ(1, table1.count("1658131670"));
//     auto &g2 = table1["1658131670"];
//     ASSERT_EQ(1658131670, g2.generationId);
//     ASSERT_EQ("config_path", g2.configPath);
//     ASSERT_EQ("/path/to/table1/2", g2.indexRoot);

//     ASSERT_EQ(0, zoneInfo.tableInfos.count("table2"));
//     ASSERT_EQ(123, zoneInfo.targetVersion);
// }

} // namespace suez
