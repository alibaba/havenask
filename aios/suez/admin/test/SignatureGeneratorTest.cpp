#include "suez/admin/SignatureGenerator.h"

#include "TargetLoader.h"
#include "autil/EnvUtil.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/md5.h"
#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace fslib;
using namespace fslib::fs;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SignatureGenerator);

class SignatureGeneratorTest : public TESTBASE {};

TEST_F(SignatureGeneratorTest, testGenerateSignautreString) {
    JsonMap jsonMap;
    ASSERT_TRUE(TargetLoader::loadFromFile(GET_TEST_DATA_PATH() + "admin_test/biz_info.json", jsonMap));

    vector<string> sigConf;
    sigConf.push_back("biz_info.$$biz_name.config_path");
    sigConf.push_back("table_info.$$table_name.$$generation_id.partitions.$$partition_id.inc_version");

    string signatureStr;
    ASSERT_TRUE(SignatureGenerator::generateSignature(sigConf, &jsonMap, signatureStr));
    EXPECT_EQ("biz1.\"config_pathXXX\" biz2.\"config_pathXXX\" table1.1458258270.0_65535.0 "
              "table2.1458281178.32768_65535.0 table3.1458281180.0_32767.0 table3.1458281180.32768_65535.0",
              signatureStr);

    sigConf.push_back("*service_info.*sub_cm2_configs");
    string signatureStr2;
    ASSERT_TRUE(SignatureGenerator::generateSignature(sigConf, &jsonMap, signatureStr2));
    EXPECT_EQ("biz1.\"config_pathXXX\" biz2.\"config_pathXXX\" table1.1458258270.0_65535.0 "
              "table2.1458281178.32768_65535.0 table3.1458281180.0_32767.0 table3.1458281180.32768_65535.0",
              signatureStr2);
}

TEST_F(SignatureGeneratorTest, testGenerateSignautreStringMinimalGsMode) {
    autil::EnvGuard env1("md5Signature", "true");
    JsonMap jsonMap;
    ASSERT_TRUE(TargetLoader::loadFromFile(GET_TEST_DATA_PATH() + "admin_test/biz_info.json", jsonMap));

    vector<string> sigConf;
    sigConf.push_back("biz_info.$$biz_name.config_path");
    sigConf.push_back("table_info.$$table_name.$$generation_id.partitions.$$partition_id.inc_version");

    string signatureStr;
    ASSERT_TRUE(SignatureGenerator::generateSignature(sigConf, &jsonMap, signatureStr));
    string tmp = "biz1.\"config_pathXXX\" biz2.\"config_pathXXX\" table1.1458258270.0_65535.0 "
                 "table2.1458281178.32768_65535.0 table3.1458281180.0_32767.0 table3.1458281180.32768_65535.0";
    Md5Stream stream;
    stream.Put((const uint8_t *)tmp.c_str(), tmp.length());

    EXPECT_EQ(stream.GetMd5String(), signatureStr);
}

TEST_F(SignatureGeneratorTest, testGenerateSignautreStringWithServiceInfo) {
    JsonMap jsonMap;
    ASSERT_TRUE(
        TargetLoader::loadFromFile(GET_TEST_DATA_PATH() + "admin_test/suez_target_with_service_info.json", jsonMap));

    vector<string> sigConf;
    sigConf.push_back("biz_info.$$biz_name.config_path");
    sigConf.push_back("table_info.$$table_name.$$generation_id.partitions.$$partition_id.inc_version");

    string signatureStr;
    ASSERT_TRUE(SignatureGenerator::generateSignature(sigConf, &jsonMap, signatureStr));
    EXPECT_EQ("biz1.\"config_pathXXX\" biz2.\"config_pathXXX\" table1.1458258270.0_65535.0 "
              "table2.1458281178.32768_65535.0 table3.1458281180.0_32767.0 table3.1458281180.32768_65535.0",
              signatureStr);

    sigConf.push_back("*service_info.*sub_cm2_configs");
    string signatureStr2;
    ASSERT_TRUE(SignatureGenerator::generateSignature(sigConf, &jsonMap, signatureStr2));
    string expect2 =
        "biz1.\"config_pathXXX\" biz2.\"config_pathXXX\" table1.1458258270.0_65535.0 table2.1458281178.32768_65535.0 "
        "table3.1458281180.0_32767.0 table3.1458281180.32768_65535.0 [\n  {\n  \"cm2_server_cluster_name\":\n    "
        "\"turing_seq_service_internal\",\n  \"cm2_server_leader_path\":\n    \"\\/suez_ops\\/cm_server_ha3\",\n  "
        "\"cm2_server_zookeeper_host\":\n    \"search-zk-cm2-ha3-ea120.vip.tbsite.net:2187\"\n  },\n  {\n  "
        "\"cm2_server_cluster_name\":\n    \"gul_be_internal_suez_ops_pre\",\n  \"cm2_server_leader_path\":\n    "
        "\"\\/suez_ops\\/cm_server_ha3\",\n  \"cm2_server_zookeeper_host\":\n    "
        "\"search-zk-cm2-ha3-ea120.vip.tbsite.net:2187\"\n  }\n]";
    EXPECT_EQ(expect2, signatureStr2);
}

TEST_F(SignatureGeneratorTest, testGenerateSignautre) {
    JsonMap jsonMap;
    ASSERT_TRUE(TargetLoader::loadFromFile(GET_TEST_DATA_PATH() + "admin_test/table_info.json", jsonMap));
    vector<string> results;

    // empty node
    ASSERT_FALSE(SignatureGenerator::generateSignature("", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre());

    // single level node
    ASSERT_TRUE(SignatureGenerator::generateSignature("table_info", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre(ToJsonString(jsonMap["table_info"])));
    results.clear();

    // single level $$
    ASSERT_TRUE(SignatureGenerator::generateSignature("$$nodes", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre("table_info"));
    results.clear();

    // multi level $$
    ASSERT_TRUE(SignatureGenerator::generateSignature("$$nodes.$$table_name", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre("table_info.table1", "table_info.table2", "table_info.table3"));
    results.clear();

    // multi level node
    ASSERT_TRUE(SignatureGenerator::generateSignature(
        "table_info.table1.1458258269.partitions.0_65535.inc_version", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre());
    results.clear();

    ASSERT_TRUE(SignatureGenerator::generateSignature(
        "table_info.table1.1458258270.partitions.0_65535.inc_version", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre("0"));
    results.clear();

    // last level is node
    ASSERT_TRUE(SignatureGenerator::generateSignature(
        "table_info.$$table_name.$$generation_id.partitions.$$partition_id.inc_version", &jsonMap, results));
    EXPECT_THAT(results,
                ElementsAre("table1.1458258270.0_65535.0",
                            "table2.1458281178.32768_65535.0",
                            "table3.1458281180.0_32767.0",
                            "table3.1458281180.32768_65535.0"));
    results.clear();

    // test optional
    ASSERT_TRUE(SignatureGenerator::generateSignature(
        "table_info.$$table_name.$$generation_id.partitions.$$partition_id.*branch_id", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre("table3.1458281180.32768_65535.100"));
    results.clear();

    // last level is $$
    ASSERT_TRUE(SignatureGenerator::generateSignature(
        "table_info.$$table_name.$$generation_id.partitions.$$partition_id", &jsonMap, results));
    EXPECT_THAT(results,
                ElementsAre("table1.1458258269.0_65535",
                            "table1.1458258270.0_65535",
                            "table2.1458281178.0_32767",
                            "table2.1458281178.32768_65535",
                            "table3.1458281180.0_32767",
                            "table3.1458281180.32768_65535"));
    results.clear();

    // biz test
    ASSERT_TRUE(TargetLoader::loadFromFile(GET_TEST_DATA_PATH() + "admin_test/biz_info.json", jsonMap));
    ASSERT_TRUE(SignatureGenerator::generateSignature("biz_info.$$biz_name.config_path", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre("biz1.\"config_pathXXX\"", "biz2.\"config_pathXXX\""));
    results.clear();

    // generate fail
    ASSERT_TRUE(TargetLoader::loadFromFile(GET_TEST_DATA_PATH() + "admin_test/gen_signature_fail.json", jsonMap));
    // key not in map
    EXPECT_TRUE(SignatureGenerator::generateSignature("biz_info.biz2.config_path", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre("{\n\"config\":\n  \"invalid\"\n}"));

    // not map
    EXPECT_FALSE(SignatureGenerator::generateSignature("biz_info.biz1.config_path", &jsonMap, results));
    // $$ not map
    EXPECT_FALSE(SignatureGenerator::generateSignature("biz_info.biz1.$$config_path", &jsonMap, results));
}

TEST_F(SignatureGeneratorTest, testGenerateSignatureIGraph) {
    JsonMap jsonMap;
    ASSERT_TRUE(TargetLoader::loadFromFile(GET_TEST_DATA_PATH() + "admin_test/app_info.json", jsonMap));
    vector<string> results;
    results.clear();
    ASSERT_TRUE(SignatureGenerator::generateSignature("app_info.config_path", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre("\"app_config_path\""));

    results.clear();
    ASSERT_TRUE(SignatureGenerator::generateSignature("app_info.#config_notexist", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre());

    results.clear();
    ASSERT_TRUE(SignatureGenerator::generateSignature("app_info.#config_notexist", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre());

    results.clear();
    ASSERT_TRUE(SignatureGenerator::generateSignature("app_info.config_notexist", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre("{\n\"config_path\":\n  \"app_config_path\"\n}"));

    results.clear();
    ASSERT_TRUE(SignatureGenerator::generateSignature("custom_app_info", &jsonMap, results));
    EXPECT_THAT(results, ElementsAre("{\n\"a\":\n  1,\n\"b\":\n  2\n}"));

    results.clear();
    ASSERT_TRUE(SignatureGenerator::generateSignature("biz_info.$$biz_name.config_path", &jsonMap, results));
    EXPECT_THAT(
        results,
        ElementsAre(
            "TPPSCHEMA_cate_price_int.{\n\"custom_biz_info\":\n  {\n  \"switch_strategy\":\n    \"default\"\n  }\n}",
            "biz1.\"config_pathXXX\""));
}

} // namespace suez
