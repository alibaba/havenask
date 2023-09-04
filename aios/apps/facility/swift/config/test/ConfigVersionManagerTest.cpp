#include "swift/config/ConfigVersionManager.h"

#include <cstddef>
#include <map>
#include <set>
#include <string>

#include "swift/config/ConfigReader.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

namespace swift {
namespace config {

using namespace swift::protocol;

using namespace std;
using namespace testing;

class ConfigVersionManagerTest : public TESTBASE {
public:
    typedef std::map<std::string, std::string> Key2ValMap;
};

TEST_F(ConfigVersionManagerTest, testNeedUpgradeRoleVersion) {
    {
        ConfigVersionManager manager;
        manager.currentBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/1/";
        EXPECT_TRUE(manager.needUpgradeRoleVersion(ROLE_TYPE_BROKER, GET_TEST_DATA_PATH() + "config_version_test/2/"));
        EXPECT_FALSE(manager.needUpgradeRoleVersion(ROLE_TYPE_BROKER, GET_TEST_DATA_PATH() + "config_version_test/3/"));

        EXPECT_FALSE(manager.needUpgradeRoleVersion(ROLE_TYPE_BROKER, GET_TEST_DATA_PATH() + "config_version_test/4/"));
        EXPECT_TRUE(manager.needUpgradeRoleVersion(ROLE_TYPE_BROKER, GET_TEST_DATA_PATH() + "config_version_test/5/"));
    }
    {
        ConfigVersionManager manager;
        manager.currentBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/6/";
        EXPECT_TRUE(manager.needUpgradeRoleVersion(ROLE_TYPE_BROKER, GET_TEST_DATA_PATH() + "config_version_test/7/"));
        EXPECT_FALSE(manager.needUpgradeRoleVersion(ROLE_TYPE_BROKER, GET_TEST_DATA_PATH() + "config_version_test/8/"));
    }
}

TEST_F(ConfigVersionManagerTest, testDiff) {
    {
        Key2ValMap kvMap;
        kvMap["abc"] = "1";
        set<string> whiteSet;
        whiteSet.insert("abc");
        ConfigVersionManager versionManager;
        EXPECT_FALSE(versionManager.hasDiff(kvMap, whiteSet));
    }
    {
        Key2ValMap kvMap;
        kvMap["abc"] = "1";
        set<string> whiteSet;
        whiteSet.insert("abcE");
        ConfigVersionManager versionManager;
        EXPECT_TRUE(versionManager.hasDiff(kvMap, whiteSet));
    }
}

TEST_F(ConfigVersionManagerTest, testDiff2) {
    Key2ValMap emptyKvMap;
    set<string> emptyWhiteSet;
    Key2ValMap kvMap;
    kvMap["abc"] = "1";
    set<string> whiteSet;
    whiteSet.insert("abc");
    ConfigVersionManager versionManager;
    { EXPECT_FALSE(versionManager.hasDiff(emptyKvMap, emptyKvMap, emptyWhiteSet)); }
    {
        EXPECT_TRUE(versionManager.hasDiff(kvMap, emptyKvMap, emptyWhiteSet));
        EXPECT_FALSE(versionManager.hasDiff(kvMap, emptyKvMap, whiteSet));
    }
    {
        EXPECT_TRUE(versionManager.hasDiff(emptyKvMap, kvMap, emptyWhiteSet));
        EXPECT_FALSE(versionManager.hasDiff(emptyKvMap, kvMap, whiteSet));
    }
    {
        EXPECT_FALSE(versionManager.hasDiff(kvMap, kvMap, whiteSet));
        EXPECT_FALSE(versionManager.hasDiff(kvMap, kvMap, emptyWhiteSet));
    }
    {
        Key2ValMap kvMap1;
        kvMap1["abc"] = "2";
        EXPECT_FALSE(versionManager.hasDiff(kvMap, kvMap1, whiteSet));
        EXPECT_TRUE(versionManager.hasDiff(kvMap, kvMap1, emptyWhiteSet));
    }
    {
        Key2ValMap kvMap1;
        kvMap1["abb"] = "1";
        EXPECT_TRUE(versionManager.hasDiff(kvMap, kvMap1, emptyWhiteSet));
        set<string> whiteSet1;
        whiteSet1.insert("abc");
        whiteSet1.insert("abb");
        EXPECT_FALSE(versionManager.hasDiff(kvMap, kvMap1, whiteSet1));
    }
    {
        Key2ValMap kvMap1;
        kvMap1["abb"] = "1";
        EXPECT_TRUE(versionManager.hasDiff(kvMap1, kvMap, emptyWhiteSet));
        set<string> whiteSet1;
        whiteSet1.insert("abc");
        whiteSet1.insert("abb");
        EXPECT_FALSE(versionManager.hasDiff(kvMap1, kvMap, whiteSet1));
    }
}

TEST_F(ConfigVersionManagerTest, testUpdateWhiteList) {
    {
        ConfigVersionManager versionManager;
        string path = GET_TEST_DATA_PATH() + "config_version_test/update_white_list/swift_no_white_list.conf";
        ConfigReader configReader;
        ASSERT_TRUE(configReader.read(path));
        map<string, set<string>> noNeedChangeList;
        string DEFAULT_COMMON_SECTION_WHITE_LIST[] = {"broker_count", "admin_count"};
        noNeedChangeList["common"] =
            set<string>(DEFAULT_COMMON_SECTION_WHITE_LIST, DEFAULT_COMMON_SECTION_WHITE_LIST + 2);
        noNeedChangeList["admin"] = set<string>();
        noNeedChangeList["broker"] = set<string>();
        versionManager.updateWhiteList(configReader, noNeedChangeList);
        EXPECT_EQ(size_t(3), noNeedChangeList.size());
        EXPECT_EQ(size_t(2), noNeedChangeList["common"].size());
    }
    {
        ConfigVersionManager versionManager;
        string path = GET_TEST_DATA_PATH() + "config_version_test/update_white_list/swift.conf";
        ConfigReader configReader;
        ASSERT_TRUE(configReader.read(path));
        map<string, set<string>> noNeedChangeList;
        string DEFAULT_COMMON_SECTION_WHITE_LIST[] = {"broker_count", "admin_count"};
        noNeedChangeList["common"] =
            set<string>(DEFAULT_COMMON_SECTION_WHITE_LIST, DEFAULT_COMMON_SECTION_WHITE_LIST + 2);
        noNeedChangeList["admin"] = set<string>();
        noNeedChangeList["broker"] = set<string>();
        versionManager.updateWhiteList(configReader, noNeedChangeList);
        EXPECT_EQ(size_t(4), noNeedChangeList.size());

        set<string> commonSet = noNeedChangeList["common"];
        EXPECT_EQ(size_t(3), commonSet.size());
        EXPECT_TRUE(commonSet.count("admin_count") > 0);
        EXPECT_TRUE(commonSet.count("broker_count") > 0);
        EXPECT_TRUE(commonSet.count("b") > 0);
        EXPECT_TRUE(noNeedChangeList.find("admin") != noNeedChangeList.end());
        EXPECT_TRUE(noNeedChangeList["admin"].empty());

        EXPECT_TRUE(noNeedChangeList.find("broker") != noNeedChangeList.end());
        set<string> brokerSet = noNeedChangeList["broker"];
        EXPECT_EQ(size_t(1), brokerSet.size());
        EXPECT_TRUE(brokerSet.count("c") > 0);

        EXPECT_TRUE(noNeedChangeList.find("unknown") != noNeedChangeList.end());
        set<string> unknownSet = noNeedChangeList["unknown"];
        EXPECT_EQ(size_t(1), unknownSet.size());
        EXPECT_TRUE(unknownSet.count("d") > 0);
    }
}

TEST_F(ConfigVersionManagerTest, testHasDiffInHippoFile) {
    string tarPath = GET_TEST_DATA_PATH() + "config_version_test/diff_hippo_file/";
    string curPath = GET_TEST_DATA_PATH() + "config_version_test/1/";
    ConfigVersionManager versionManager;
    ASSERT_TRUE(versionManager.hasDiffInHippoFile(curPath, tarPath));
    ASSERT_FALSE(versionManager.hasDiffInHippoFile(curPath, curPath));
}

TEST_F(ConfigVersionManagerTest, testValidateUpdateVersion) {
    {
        ConfigVersionManager versionManager;
        versionManager.brokerRollback = true;
        ASSERT_EQ(ERROR_BROKER_IS_UPGRADING, versionManager.validateUpdateVersion(ROLE_TYPE_BROKER, "updatePath"));
    }
    {
        ConfigVersionManager versionManager;
        versionManager.currentBrokerConfigPath = "curPath";
        versionManager.targetBrokerConfigPath = "tarPath";
        ASSERT_EQ(ERROR_BROKER_IS_UPGRADING, versionManager.validateUpdateVersion(ROLE_TYPE_BROKER, "updatePath"));
    }
    {
        ConfigVersionManager versionManager;
        versionManager.currentBrokerConfigPath = "curPath";
        versionManager.targetBrokerConfigPath = "curPath";
        ASSERT_EQ(ERROR_UPGRADE_VERSION_SAME_AS_CURRENT,
                  versionManager.validateUpdateVersion(ROLE_TYPE_BROKER, "curPath"));
    }
    {
        ConfigVersionManager versionManager;
        versionManager.currentBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/empty_conf/";
        versionManager.targetBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/empty_conf/";
        string updateVersionPath = GET_TEST_DATA_PATH() + "config_version_test/";
        ASSERT_EQ(ERROR_ADMIN_APPLICATION_ID_CHANGED,
                  versionManager.validateUpdateVersion(ROLE_TYPE_BROKER, updateVersionPath));
    }
    {
        ConfigVersionManager versionManager;
        versionManager.currentBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/";
        versionManager.targetBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/";
        string updateVersionPath = GET_TEST_DATA_PATH() + "config_version_test/empty_conf/";
        ASSERT_EQ(ERROR_ADMIN_APPLICATION_ID_CHANGED,
                  versionManager.validateUpdateVersion(ROLE_TYPE_BROKER, updateVersionPath));
    }
    {
        ConfigVersionManager versionManager;
        versionManager.currentBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/";
        versionManager.targetBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/";
        string updateVersionPath = GET_TEST_DATA_PATH() + "config_version_test/empty_conf/";
        ASSERT_EQ(ERROR_ADMIN_APPLICATION_ID_CHANGED,
                  versionManager.validateUpdateVersion(ROLE_TYPE_BROKER, updateVersionPath));
    }
    {
        ConfigVersionManager versionManager;
        versionManager.currentBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/validate_update_version_1";
        versionManager.targetBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/validate_update_version_1";
        string updateVersionPath = GET_TEST_DATA_PATH() + "config_version_test/validate_update_version_2";
        ASSERT_EQ(ERROR_ADMIN_APPLICATION_ID_CHANGED,
                  versionManager.validateUpdateVersion(ROLE_TYPE_BROKER, updateVersionPath));
    }
    {
        ConfigVersionManager versionManager;
        versionManager.currentBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/normal_swift_conf";
        versionManager.targetBrokerConfigPath = GET_TEST_DATA_PATH() + "config_version_test/normal_swift_conf";
        string updateVersionPath = GET_TEST_DATA_PATH() + "config_version_test/normal_swift_conf_1";
        ASSERT_EQ(ERROR_NONE, versionManager.validateUpdateVersion(ROLE_TYPE_BROKER, updateVersionPath));
    }
}

TEST_F(ConfigVersionManagerTest, testValidateRollback) {
    {
        ConfigVersionManager versionManager;
        versionManager.brokerRollback = true;
        ASSERT_EQ(ERROR_CONFIG_IS_ROLLBACKING, versionManager.validateRollback(ROLE_TYPE_BROKER));
    }
    {
        ConfigVersionManager versionManager;
        ASSERT_EQ(ERROR_NOT_NEED_ROLLBACK, versionManager.validateRollback(ROLE_TYPE_BROKER));
    }
    {
        ConfigVersionManager versionManager;
        versionManager.currentBrokerConfigPath = "conf";
        ASSERT_EQ(ERROR_NONE, versionManager.validateRollback(ROLE_TYPE_BROKER));
    }
}

} // namespace config
} // namespace swift
