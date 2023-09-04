#include "swift/common/PathDefine.h"

#include <iosfwd>
#include <stdint.h>
#include <stdlib.h>
#include <string>

#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

namespace swift {
namespace common {

using namespace std;
using namespace fslib::fs;

class PathDefineTest : public TESTBASE {};

TEST_F(PathDefineTest, testGetAddress) {
    EXPECT_EQ(string("10.250.12.21:123"), PathDefine::getAddress("10.250.12.21", 123));

    EXPECT_EQ(string("10.250.12.21:123"), PathDefine::getAddress("10.250.12.21  ", 123));

    EXPECT_EQ(string("10.250.12.21:123"), PathDefine::getAddress("   10.250.12.21  ", 123));
}

TEST_F(PathDefineTest, testHeartbeatReportAddress) {
    EXPECT_EQ("zfs://10.250.12.21:2181/w/heartbeat/10.250.12.23:123",
              PathDefine::heartbeatReportAddress("zfs://10.250.12.21:2181/w", " 10.250.12.23:123 "));
    EXPECT_EQ("zfs://10.250.12.21:2181/w/heartbeat/10.250.12.23:123",
              PathDefine::heartbeatReportAddress("zfs://10.250.12.21:2181/w/", " 10.250.12.23:123 "));
}

TEST_F(PathDefineTest, testHeartbeatMonitorAddress) {
    EXPECT_EQ("zfs://10.250.12.21:2181/w/heartbeat", PathDefine::heartbeatMonitorAddress("zfs://10.250.12.21:2181/w"));
    EXPECT_EQ("zfs://10.250.12.21:2181/w/heartbeat", PathDefine::heartbeatMonitorAddress("zfs://10.250.12.21:2181/w/"));
}

TEST_F(PathDefineTest, testParseSpec) {
    string spec = "tcp:10.250.12.2:123";
    string ip;
    uint16_t port = 0;
    EXPECT_TRUE(PathDefine::parseAddress(spec, ip, port));
    EXPECT_EQ(string("10.250.12.2"), ip);
    EXPECT_EQ(uint16_t(123), port);
}

TEST_F(PathDefineTest, testParseAddress) {
    string address = "10.250.12.2:123";
    string ip;
    uint16_t port = 0;
    EXPECT_TRUE(PathDefine::parseAddress(address, ip, port));
    EXPECT_EQ(string("10.250.12.2"), ip);
    EXPECT_EQ(uint16_t(123), port);
}

TEST_F(PathDefineTest, testParseWrongAddress) {
    string address = "10.250.12.2:123a";
    string ip;
    uint16_t port = 0;
    EXPECT_TRUE(!PathDefine::parseAddress(address, ip, port));

    address = "10.250.12.2:";
    EXPECT_TRUE(!PathDefine::parseAddress(address, ip, port));

    address = "10.250.12.2: 12";
    EXPECT_TRUE(!PathDefine::parseAddress(address, ip, port));

    address = "udp:10.250.12.2: 12";
    EXPECT_TRUE(!PathDefine::parseAddress(address, ip, port));

    address = ":10.250.12.2:123";
    EXPECT_TRUE(!PathDefine::parseAddress(address, ip, port));
}

TEST_F(PathDefineTest, testGetPath) {
    string zkPath = "zfs://1.1.1.1/path";
    EXPECT_EQ("zfs://1.1.1.1/path/partition_info", PathDefine::getPartitionInfoFile(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/topic_meta", PathDefine::getTopicMetaFile(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/topic_rwinfo", PathDefine::getTopicRWFile(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/tasks", PathDefine::getTaskDir(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/tasks/role", PathDefine::getTaskInfoPath(zkPath, "role"));
    EXPECT_EQ("zfs://1.1.1.1/path/topics", PathDefine::getTopicDir(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/schema", PathDefine::getSchemaDir(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/schema/topica", PathDefine::getTopicSchemaFile(zkPath, "topica"));
    EXPECT_EQ("zfs://1.1.1.1/path/topics/topica/meta", PathDefine::getTopicMetaPath(zkPath, "topica"));
    EXPECT_EQ("zfs://1.1.1.1/path/topics/topic/partitions/3", PathDefine::getTopicPartitionPath(zkPath, "topic", 3));
    EXPECT_EQ("zfs://1.1.1.1/path/admin/leader_history", PathDefine::getLeaderHistoryFile(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/admin/leader_info", PathDefine::getLeaderInfoFile(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/config/version", PathDefine::getConfigVersionFile(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/nouse_topics", PathDefine::getNoUseTopicDir(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/change_partcnt_tasks", PathDefine::getChangePartCntTaskFile(zkPath));
    EXPECT_EQ("zfs://1.1.1.1/path/write_version_control", PathDefine::getWriteVersionControlPath(zkPath));
}

TEST_F(PathDefineTest, testGenerateAccessKey) {
    EXPECT_EQ("abc__", PathDefine::generateAccessKey("abc"));
    setenv("HIPPO_APP_SLOT_ID", "1234", 1);
    setenv("HIPPO_APP_WORKDIR", "/123/12", 1);
    EXPECT_EQ("abc_1234_/123/12", PathDefine::generateAccessKey("abc"));
}

TEST_F(PathDefineTest, testParseRoleAddress) {
    string role, address;
    string roleAddress = "default##broker_249_61#_#11.14.222.104:56933";
    bool ret = PathDefine::parseRoleAddress(roleAddress, role, address);
    EXPECT_TRUE(ret);
    EXPECT_EQ("default##broker_249_61", role);
    EXPECT_EQ("11.14.222.104:56933", address);

    roleAddress = "default##broker_249_61##11.14.222.104:56933";
    ret = PathDefine::parseRoleAddress(roleAddress, role, address);
    EXPECT_FALSE(ret);

    // abnormal
    roleAddress = "default##broker-249-61#_#11.14.222.104:56933";
    ret = PathDefine::parseRoleAddress(roleAddress, role, address);
    EXPECT_TRUE(ret);
    EXPECT_EQ("default##broker-249-61", role);
    EXPECT_EQ("11.14.222.104:56933", address);
}

TEST_F(PathDefineTest, testParseRoleGroupAndIndex) {
    string group;
    uint32_t index = 0;
    string roleName = "default##broker_249_61";
    bool ret = PathDefine::parseRoleGroupAndIndex(roleName, group, index);
    EXPECT_TRUE(ret);
    EXPECT_EQ("default", group);
    EXPECT_EQ(249, index);

    roleName = "other##broker_0_1";
    ret = PathDefine::parseRoleGroupAndIndex(roleName, group, index);
    EXPECT_TRUE(ret);
    EXPECT_EQ("other", group);
    EXPECT_EQ(0, index);

    // error
    roleName = "other#broker_0_1";
    EXPECT_FALSE(PathDefine::parseRoleGroupAndIndex(roleName, group, index));
    roleName = "other##broker0-0_1";
    EXPECT_FALSE(PathDefine::parseRoleGroupAndIndex(roleName, group, index));
}

TEST_F(PathDefineTest, testWriteRecommendPort) {
    uint32_t rpcPort = 0, httpPort = 0;
    EXPECT_TRUE(PathDefine::writeRecommendPort(100, 200));
    EXPECT_TRUE(PathDefine::readRecommendPort(rpcPort, httpPort));
    EXPECT_EQ(100, rpcPort);
    EXPECT_EQ(200, httpPort);
}

TEST_F(PathDefineTest, testReadRecommendPort) {
    uint32_t rpcPort = 0, httpPort = 0;
    // 1. not exist
    EXPECT_EQ(fslib::EC_OK, FileSystem::removeFile("ports"));
    EXPECT_FALSE(PathDefine::readRecommendPort(rpcPort, httpPort));
    // 2. port file illegal
    EXPECT_EQ(fslib::EC_OK, FileSystem::writeFile("ports", "aaaa"));
    EXPECT_FALSE(PathDefine::readRecommendPort(rpcPort, httpPort));
    // 3. port file ok
    EXPECT_EQ(fslib::EC_OK, FileSystem::writeFile("ports", "22,33"));
    EXPECT_TRUE(PathDefine::readRecommendPort(rpcPort, httpPort));
    EXPECT_EQ(22, rpcPort);
    EXPECT_EQ(33, httpPort);
}

} // namespace common
} // namespace swift
