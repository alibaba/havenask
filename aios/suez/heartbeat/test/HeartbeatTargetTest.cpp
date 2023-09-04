#include "suez/heartbeat/HeartbeatTarget.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <map>
#include <string>
#include <utility>

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "suez/common/TableMeta.h"
#include "suez/sdk/BizMeta.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil::legacy::json;

namespace suez {

class HeartbeatTargetTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void HeartbeatTargetTest::setUp() {}

void HeartbeatTargetTest::tearDown() {}

TEST_F(HeartbeatTargetTest, testSimple) {
    HeartbeatTarget target;
    ASSERT_FALSE(target.parseFrom("", ""));
    string content;
    string testPath = GET_TEST_DATA_PATH() + "heartbeat_test/";
    string testFile = testPath + "target.json";
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(testFile, content));
    string signature = "signature";
    ASSERT_TRUE(target.parseFrom(content, signature));
    ASSERT_EQ(signature, target.getSignature());
    string expectAppConfigPath =
        "hdfs://hdpet2main/app/suez_ops/online_config/mainse_online/app/app_info/config/1480647478";
    ASSERT_EQ(expectAppConfigPath, target.getAppMeta().getRemoteConfigPath());
    ASSERT_EQ(3, target.getAppMeta().getKeepCount());
    const JsonMap &customAppInfo = target.getCustomAppInfo();
    ASSERT_EQ("{\"a\":\"b\",\"c\":1}", FastToJsonString(customAppInfo, true));

    string expectConfigPath =
        "hdfs://hdpet2main/app/suez_ops/online_config/mainse_online/biz/default/config/1480647479";
    ASSERT_EQ(1u, target.getBizMetas().size());
    const auto t = target.getBizMetas().find("default");
    ASSERT_TRUE(t != target.getBizMetas().end());
    ASSERT_EQ(expectConfigPath, t->second.getRemoteConfigPath());
    PartitionId pid1;
    pid1.tableId.tableName = "mainse_excellent";
    pid1.tableId.fullVersion = 1480625075;
    pid1.from = 0;
    pid1.to = 16383;

    PartitionMeta meta1;
    meta1.setIndexRoot("hdfs://hdpst3bts/app/suez_ops/index/mainse_online/mainse/index");
    meta1.setConfigPath(
        "hdfs://hdpet2main/app/suez_ops/online_config/mainse_online/table/mainse_excellent/config/1478600179");
    meta1.incVersion = 36;
    meta1.tableStatus = TS_UNLOADED;

    ASSERT_EQ(1, target.getTableMetas().size());
    const auto tableMeta = target.getTableMetas().find(pid1);
    ASSERT_TRUE(tableMeta != target.getTableMetas().end());
    ASSERT_EQ(meta1, tableMeta->second);

    meta1.tableMeta.configPath = "";
    target._target["table_info"] = ToJson(meta1);
    ASSERT_FALSE(target.parseFrom(FastToJsonString(target._target), ""));
}

TEST_F(HeartbeatTargetTest, testCheckTarget) {
    HeartbeatTarget heartbeatTarget;
    PartitionId pid;
    pid.tableId.tableName = "tableName";
    pid.tableId.fullVersion = 12345;
    pid.from = 0;
    pid.to = 65536;
    PartitionMeta partMeta;
    partMeta.incVersion = 1;
    partMeta.tableMeta.rtStatus = TRS_SUSPENDED;
    partMeta.setIndexRoot("indexRoot");
    partMeta.setConfigPath("configPath");
    heartbeatTarget._tableMetas[pid] = partMeta;
    heartbeatTarget._tableMetas[pid].tableStatus = TS_UNLOADED;

    EXPECT_TRUE(heartbeatTarget.checkTarget());

    TableMetas &target = heartbeatTarget._tableMetas;
    target[pid].setIndexRoot("");
    EXPECT_FALSE(heartbeatTarget.checkTarget());

    target[pid].setIndexRoot("indexRoot");
    target[pid].setConfigPath("");
    EXPECT_FALSE(heartbeatTarget.checkTarget());

    target[pid].setConfigPath("configPath");
    target[pid].incVersion = -1;
    EXPECT_FALSE(heartbeatTarget.checkTarget());

    target[pid].incVersion = 1;
    PartitionId pid2 = pid;
    pid2.from = 1234;

    target[pid2] = target[pid];
    EXPECT_TRUE(heartbeatTarget.checkTarget());

    target[pid2].incVersion = -1;
    EXPECT_FALSE(heartbeatTarget.checkTarget());
}

} // namespace suez
