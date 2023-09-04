#include "swift/admin/AdminZkDataAccessor.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>
#include <zlib.h>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "ext/alloc_traits.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/common/PathDefine.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h"
#include "unittest/unittest.h"

using namespace swift::protocol;
using namespace fslib;
using namespace fslib::fs;
using namespace std;
using namespace autil;

namespace swift {
namespace admin {

class AdminZkDataAccessorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    void createLegacyTopicInfos(const string &topicName, uint32_t partCount, uint32_t bufferSize);

protected:
    cm_basic::ZkWrapper *_zkWrapper = nullptr;
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;

private:
    void
    expectZk(const string &topicName, uint32_t count, const vector<int32_t> &versions, const vector<string> &schemas);
};

void AdminZkDataAccessorTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    string zkPath = "zfs://" + oss.str() + "/";
    _zkWrapper = new cm_basic::ZkWrapper(oss.str(), 1000);
    ASSERT_TRUE(_zkWrapper->open());
    _zkRoot = zkPath + GET_CLASS_NAME();
    cout << "zkRoot: " << _zkRoot << endl;
    fs::FileSystem::mkDir(_zkRoot);
}

void AdminZkDataAccessorTest::tearDown() { DELETE_AND_SET_NULL(_zkWrapper); }

TEST_F(AdminZkDataAccessorTest, testOperateLeaderInfo) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));

    LeaderInfo leaderInfo;
    leaderInfo.set_address("0.0.0.1:1111");
    leaderInfo.set_starttimesec(123456789);
    leaderInfo.set_sysstop(true);
    ASSERT_TRUE(da.setLeaderInfo(leaderInfo));

    LeaderInfo actual;
    ASSERT_TRUE(da.getLeaderInfo(actual));
    EXPECT_EQ(leaderInfo.address(), actual.address());
    EXPECT_EQ(leaderInfo.starttimesec(), actual.starttimesec());
    EXPECT_EQ(leaderInfo.sysstop(), actual.sysstop());

    LeaderInfo jsonLeaderInfo;
    string jsonPath = common::PathDefine::getLeaderInfoJsonFile("/" + GET_CLASS_NAME());
    ASSERT_TRUE(da.readZk(jsonPath, jsonLeaderInfo, false, true));
    EXPECT_EQ(leaderInfo.address(), actual.address());
    EXPECT_EQ(leaderInfo.starttimesec(), actual.starttimesec());
    EXPECT_EQ(leaderInfo.sysstop(), actual.sysstop());
}

TEST_F(AdminZkDataAccessorTest, testOperateLeaderHistory) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));

    for (size_t i = 0; i < 105; ++i) {
        LeaderInfo leaderInfo;
        leaderInfo.set_address("12345");
        leaderInfo.set_starttimesec(i);
        leaderInfo.set_sysstop(true);
        ASSERT_TRUE(da.addLeaderInfoToHistory(leaderInfo));
    }

    LeaderInfoHistory actual;
    ASSERT_TRUE(da.getHistoryLeaders(actual));
    ASSERT_EQ(100, actual.leaderinfos_size());
    for (int i = 0; i < 100; i++) {
        EXPECT_EQ("12345", actual.leaderinfos(i).address());
        EXPECT_EQ(105 - i - 1, actual.leaderinfos(i).starttimesec());
        EXPECT_EQ(true, actual.leaderinfos(i).sysstop());
    }
}

TEST_F(AdminZkDataAccessorTest, testLoadTopicInfos) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));

    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/topic_meta"));

    TopicCreationRequest req;
    req.set_topicname("topic1");
    req.set_partitioncount(3);
    req.set_partitionbuffersize(1024);

    ASSERT_TRUE(da.addTopic(req));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/topic_meta"));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/partition_info"));

    AdminZkDataAccessor da2;
    ASSERT_TRUE(da2.init(_zkWrapper, _zkRoot));

    TopicMetas topicMetas;
    TopicPartitionInfos topicPartInfos;
    ASSERT_TRUE(da2.loadTopicInfos(topicMetas, topicPartInfos));
    ASSERT_EQ(1, topicMetas.topicmetas_size());
    ASSERT_EQ(1, topicPartInfos.topicpartitioninfos_size());
    ASSERT_EQ(string("topic1"), topicMetas.topicmetas(0).topicname());
    ASSERT_EQ(uint32_t(3), topicMetas.topicmetas(0).partitioncount());
    ASSERT_EQ(uint32_t(1024), topicMetas.topicmetas(0).partitionbuffersize());
    ASSERT_EQ(string("topic1"), topicPartInfos.topicpartitioninfos(0).topicname());
    ASSERT_EQ(3, topicPartInfos.topicpartitioninfos(0).partitioninfos_size());
    for (int i = 0; i < topicPartInfos.topicpartitioninfos(0).partitioninfos_size(); i++) {
        const PartitionInfo &pi = topicPartInfos.topicpartitioninfos(0).partitioninfos(i);
        ASSERT_EQ(uint32_t(i), pi.id());
        ASSERT_EQ(string(""), pi.brokeraddress());
        ASSERT_EQ(PARTITION_STATUS_NONE, pi.status());
    }
}

TEST_F(AdminZkDataAccessorTest, testBrokerVersionFile) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));

    RoleVersionInfo roleInfo;
    roleInfo.set_rolename("aaa");
    RoleVersionInfos roleInfos;
    *roleInfos.add_versioninfos() = roleInfo;
    ASSERT_TRUE(da.setBrokerVersionInfos(roleInfos));
    RoleVersionInfos roleInfos2;
    ASSERT_TRUE(da.getBrokerVersionInfos(roleInfos2));
    ASSERT_TRUE(roleInfos == roleInfos2);
}

TEST_F(AdminZkDataAccessorTest, testLoadTopicInfosLegacy) {
    fs::FileSystem::mkDir(_zkRoot + "/topics");
    string topicName1 = "1";
    string topicName2 = "2";
    createLegacyTopicInfos(topicName1, 3, 20);
    createLegacyTopicInfos(topicName2, 2, 60);

    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));

    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/topic_meta"));
    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/partition_info"));

    TopicMetas topicMetas;
    TopicPartitionInfos topicPartInfos;
    ASSERT_TRUE(da.loadTopicInfos(topicMetas, topicPartInfos));
    ASSERT_EQ(2, topicMetas.topicmetas_size()) << topicMetas.ShortDebugString();
    ASSERT_EQ(2, topicPartInfos.topicpartitioninfos_size()) << topicPartInfos.ShortDebugString();

    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/topic_meta"));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/partition_info"));
    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/topics"));
    ASSERT_EQ(2, topicMetas.topicmetas_size());
    ASSERT_EQ(topicName1, topicMetas.topicmetas(0).topicname());
    ASSERT_EQ(topicName2, topicMetas.topicmetas(1).topicname());

#define CHECK_TOPIC(topicMeta, topicPartitionInfo)                                                                     \
    do {                                                                                                               \
        ASSERT_EQ(topicMeta.topicname(), topicPartitionInfo.topicname());                                              \
        ASSERT_EQ(topicMeta.partitioncount(), (uint32_t)topicPartitionInfo.partitioninfos_size());                     \
        for (uint32_t i = 0; i < topicMeta.partitioncount(); i++) {                                                    \
            const PartitionInfo &pi = topicPartitionInfo.partitioninfos(i);                                            \
            ASSERT_EQ(string("1.1.1.") + StringUtil::toString(pi.id()) + ":1234", pi.brokeraddress());                 \
            ASSERT_EQ(PARTITION_STATUS_NONE, pi.status());                                                             \
        }                                                                                                              \
    } while (0)

    for (int i = 0; i < topicMetas.topicmetas_size(); i++) {
        const TopicCreationRequest &meta = topicMetas.topicmetas(i);
        const TopicPartitionInfo &partInfo = topicPartInfos.topicpartitioninfos(i);
        CHECK_TOPIC(meta, partInfo);
    }
#undef CHECK_TOPIC
}

/**
 * legacy topic infos:
 *  $root/topics/topic_a/meta
 *  $root/topics/topic_a/partitions/0
 *  $root/topics/topic_a/partitions/1
 *  $root/topics/topic_b/meta
 *  $root/topics/topic_b/partitions/0
 *  $root/topics/topic_b/partitions/1
 */
void AdminZkDataAccessorTest::createLegacyTopicInfos(const string &topicName, uint32_t partCount, uint32_t bufferSize) {
    TopicCreationRequest req;
    req.set_topicname(topicName);
    req.set_partitioncount(partCount);
    req.set_partitionbuffersize(bufferSize);

    string topicRootDir = _zkRoot + "/topics";
    if (fs::FileSystem::isExist(topicRootDir) != EC_TRUE) {
        fs::FileSystem::mkDir(topicRootDir);
    }
    string topicDir = topicRootDir + "/" + topicName;
    fs::FileSystem::mkDir(topicDir);
    string data;
    req.SerializeToString(&data);
    ASSERT_EQ(fslib::EC_OK, FileSystem::writeFile(topicDir + "/meta", data));
    string partitionDir = topicDir + "/partitions";

    for (uint32_t i = 0; i < partCount; i++) {
        PartitionInfo pi;
        pi.set_id(0);
        pi.set_brokeraddress("1.1.1." + StringUtil::toString(i) + ":1234");
        pi.set_status(PARTITION_STATUS_NONE);
        string data;
        pi.SerializeToString(&data);
        ASSERT_EQ(fslib::EC_OK, FileSystem::writeFile(partitionDir + "/" + StringUtil::toString(i), data));
    }
}

TEST_F(AdminZkDataAccessorTest, testReadTopicMetas) {
    AdminZkDataAccessor zda;
    string data;
    string metaPath = _zkRoot + "/topic_meta";

    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(metaPath));
    ASSERT_FALSE(zda.readTopicMetas(data));

    string content("aabbccdd");
    ASSERT_EQ(fslib::EC_OK, fs::FileSystem::writeFile(metaPath, content));
    ASSERT_TRUE(zda.init(_zkWrapper, _zkRoot));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(metaPath));

    ASSERT_TRUE(zda.readTopicMetas(data));
    ASSERT_EQ(data, content);
}

TEST_F(AdminZkDataAccessorTest, testAddTopics) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));
    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/topic_meta"));

    TopicBatchCreationRequest batchRequest;
    const size_t TOPIC_NUM = 10;
    for (size_t count = 0; count < TOPIC_NUM; ++count) {
        TopicCreationRequest *req = batchRequest.add_topicrequests();
        string topicName = string("topic_") + std::to_string(count);
        req->set_topicname(topicName);
        req->set_partitioncount(count + 1);
        req->set_partitionbuffersize(count + 1);
    }

    ASSERT_TRUE(da.addTopics(batchRequest));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/topic_meta"));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/partition_info"));

    AdminZkDataAccessor da2;
    ASSERT_TRUE(da2.init(_zkWrapper, _zkRoot));

    TopicMetas topicMetas;
    TopicPartitionInfos topicPartInfos;
    ASSERT_TRUE(da2.loadTopicInfos(topicMetas, topicPartInfos));
    ASSERT_EQ(TOPIC_NUM, topicMetas.topicmetas_size());
    ASSERT_EQ(TOPIC_NUM, topicPartInfos.topicpartitioninfos_size());
    for (size_t count = 0; count < TOPIC_NUM; ++count) {
        ASSERT_EQ(string("topic_") + std::to_string(count), topicMetas.topicmetas(count).topicname());
        ASSERT_EQ(count + 1, topicMetas.topicmetas(count).partitioncount());
        ASSERT_EQ(count + 1, topicMetas.topicmetas(count).partitionbuffersize());
        ASSERT_EQ(string("topic_") + std::to_string(count), topicPartInfos.topicpartitioninfos(count).topicname());
        ASSERT_EQ(count + 1, topicPartInfos.topicpartitioninfos(count).partitioninfos_size());
        for (int i = 0; i < topicPartInfos.topicpartitioninfos(count).partitioninfos_size(); i++) {
            const PartitionInfo &pi = topicPartInfos.topicpartitioninfos(count).partitioninfos(i);
            ASSERT_EQ(uint32_t(i), pi.id());
            ASSERT_EQ(string(""), pi.brokeraddress());
            ASSERT_EQ(PARTITION_STATUS_NONE, pi.status());
        }
    }
}

TEST_F(AdminZkDataAccessorTest, testGetOldestSchemaVersion) {
    AdminZkDataAccessor da;
    SchemaInfos scmInfos;
    int32_t version = 0;
    int pos = -1;

    EXPECT_TRUE(da.getOldestSchemaVersion(scmInfos, version, pos));
    EXPECT_EQ(0, version);
    EXPECT_EQ(-1, pos);

    SchemaInfo *sc = scmInfos.add_sinfos();
    sc->set_version(100);
    sc->set_registertime(100);
    sc->set_schema("xxx");
    EXPECT_TRUE(da.getOldestSchemaVersion(scmInfos, version, pos));
    EXPECT_EQ(100, version);
    EXPECT_EQ(0, pos);

    sc = scmInfos.add_sinfos();
    sc->set_version(200);
    sc->set_registertime(50);
    sc->set_schema("yyy");
    EXPECT_TRUE(da.getOldestSchemaVersion(scmInfos, version, pos));
    EXPECT_EQ(200, version);
    EXPECT_EQ(1, pos);
}

TEST_F(AdminZkDataAccessorTest, testWriteTopicSchema) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/schema"));
    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/schema/topica"));
    SchemaInfos scmInfos;
    EXPECT_TRUE(da.writeTopicSchema("topica", scmInfos));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/schema/topica"));
    string content1, content2;
    ASSERT_EQ(EC_OK, fs::FileSystem::readFile(_zkRoot + "/schema/topica", content1));

    SchemaInfo *sc = scmInfos.add_sinfos();
    sc->set_version(100);
    sc->set_registertime(200);
    sc->set_schema("xxx");
    EXPECT_TRUE(da.writeTopicSchema("topica", scmInfos));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/schema/topica"));
    ASSERT_EQ(EC_OK, fs::FileSystem::readFile(_zkRoot + "/schema/topica", content2));

    EXPECT_TRUE(content1 != content2);
    autil::ZlibCompressor compressor(Z_BEST_COMPRESSION);
    std::string decompressedData;
    EXPECT_TRUE(compressor.decompress(content2, decompressedData));

    SchemaInfos result;
    EXPECT_TRUE(result.ParseFromString(decompressedData));
    EXPECT_EQ(1, result.sinfos_size());
    EXPECT_EQ(100, result.sinfos(0).version());
    EXPECT_EQ(200, result.sinfos(0).registertime());
    EXPECT_EQ(string("xxx"), result.sinfos(0).schema());
}

void AdminZkDataAccessorTest::expectZk(const string &topicName,
                                       uint32_t count,
                                       const vector<int32_t> &versions,
                                       const vector<string> &schemas) {
    string content;
    EXPECT_EQ(EC_OK, fs::FileSystem::readFile(_zkRoot + "/schema/" + topicName, content));
    autil::ZlibCompressor compressor(Z_BEST_COMPRESSION);
    std::string decompressedData;
    EXPECT_TRUE(compressor.decompress(content, decompressedData));

    SchemaInfos result;
    EXPECT_TRUE(result.ParseFromString(decompressedData));
    EXPECT_EQ(count, result.sinfos_size());
    for (uint32_t index = 0; index < count; ++index) {
        EXPECT_EQ(versions[index], result.sinfos(index).version());
        cout << "registerTime: " << result.sinfos(index).registertime() << endl;
        EXPECT_EQ(schemas[index], result.sinfos(index).schema());
    }
}

void expectTopicSchemas(const SchemaInfos &scinfos,
                        uint32_t count,
                        const vector<int32_t> &versions,
                        const vector<string> &schemas) {
    EXPECT_EQ(count, scinfos.sinfos_size());
    for (uint32_t index = 0; index < count; ++index) {
        EXPECT_EQ(versions[index], scinfos.sinfos(index).version());
        cout << "registerTime: " << scinfos.sinfos(index).registertime() << endl;
        EXPECT_EQ(schemas[index], scinfos.sinfos(index).schema());
    }
}

TEST_F(AdminZkDataAccessorTest, testAddTopicSchema) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));
    EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/schema"));
    int32_t removedVersion = 0;
    int32_t maxAllowSchemaNum = 2;

    EXPECT_TRUE(da.addTopicSchema("topic1", 100, "xxx", removedVersion, maxAllowSchemaNum));
    EXPECT_EQ(1, da._topicSchemas.size());
    vector<int32_t> expectVersions = {100};
    vector<string> expectSchemas = {"xxx"};
    expectZk("topic1", 1, expectVersions, expectSchemas);
    expectTopicSchemas(da._topicSchemas["topic1"], 1, expectVersions, expectSchemas);

    sleep(1);
    EXPECT_TRUE(da.addTopicSchema("topic1", 200, "yyy", removedVersion, maxAllowSchemaNum));
    expectVersions.push_back(200);
    expectSchemas.push_back("yyy");
    expectZk("topic1", 2, expectVersions, expectSchemas);
    expectTopicSchemas(da._topicSchemas["topic1"], 2, expectVersions, expectSchemas);

    sleep(1);
    EXPECT_TRUE(da.addTopicSchema("topic1", 300, "zzz", removedVersion, maxAllowSchemaNum));
    expectVersions[0] = 300;
    expectSchemas[0] = "zzz";
    expectZk("topic1", 2, expectVersions, expectSchemas);
    expectTopicSchemas(da._topicSchemas["topic1"], 2, expectVersions, expectSchemas);

    sleep(1);
    EXPECT_TRUE(da.addTopicSchema("topic1", 400, "ddd", removedVersion, maxAllowSchemaNum));
    expectVersions[1] = 400;
    expectSchemas[1] = "ddd";
    expectZk("topic1", 2, expectVersions, expectSchemas);
    expectTopicSchemas(da._topicSchemas["topic1"], 2, expectVersions, expectSchemas);

    _zkWrapper->close(); // write zk will fail

    sleep(1);
    EXPECT_FALSE(da.addTopicSchema("topic1", 500, "eee", removedVersion, maxAllowSchemaNum));
    expectZk("topic1", 2, expectVersions, expectSchemas);
    expectTopicSchemas(da._topicSchemas["topic1"], 2, expectVersions, expectSchemas);
}

TEST_F(AdminZkDataAccessorTest, testGetSchema) {
    AdminZkDataAccessor da;

    SchemaInfo schemaInfo;
    EXPECT_FALSE(da.getSchema("topic1", 100, schemaInfo));
    EXPECT_FALSE(schemaInfo.has_version());
    EXPECT_FALSE(schemaInfo.has_schema());

    SchemaInfos scmInfos;
    SchemaInfo *sc = scmInfos.add_sinfos();
    sc->set_version(100);
    sc->set_registertime(200);
    sc->set_schema("xxx");
    da._topicSchemas["topic1"] = scmInfos;

    { // wrong version
        SchemaInfo schemaInfo;
        EXPECT_FALSE(da.getSchema("topic1", 200, schemaInfo));
        EXPECT_FALSE(schemaInfo.has_version());
        EXPECT_FALSE(schemaInfo.has_schema());
        EXPECT_FALSE(schemaInfo.has_registertime());
    }

    { // right version
        SchemaInfo schemaInfo;
        EXPECT_TRUE(da.getSchema("topic1", 100, schemaInfo));
        EXPECT_EQ(100, schemaInfo.version());
        EXPECT_EQ(200, schemaInfo.registertime());
        EXPECT_EQ("xxx", schemaInfo.schema());
    }

    { // wrong topic
        SchemaInfo schemaInfo;
        EXPECT_FALSE(da.getSchema("topic2", 100, schemaInfo));
        EXPECT_FALSE(schemaInfo.has_version());
        EXPECT_FALSE(schemaInfo.has_registertime());
        EXPECT_FALSE(schemaInfo.has_schema());
    }

    { // latest version
        SchemaInfo schemaInfo;
        EXPECT_TRUE(da.getSchema("topic1", 0, schemaInfo));
        EXPECT_EQ(100, schemaInfo.version());
        EXPECT_EQ(200, schemaInfo.registertime());
        EXPECT_EQ("xxx", schemaInfo.schema());
    }

    sc = scmInfos.add_sinfos();
    sc->set_version(200);
    sc->set_registertime(300);
    sc->set_schema("yyy");
    da._topicSchemas["topic1"] = scmInfos;

    { // latest version
        SchemaInfo schemaInfo;
        EXPECT_TRUE(da.getSchema("topic1", 0, schemaInfo));
        EXPECT_EQ(200, schemaInfo.version());
        EXPECT_EQ(300, schemaInfo.registertime());
        EXPECT_EQ("yyy", schemaInfo.schema());
    }
}

TEST_F(AdminZkDataAccessorTest, testLoadTopicSchemas) {
    AdminZkDataAccessor da;

    // schema path not exist
    EXPECT_FALSE(da.loadTopicSchemas());

    // empty schemas
    EXPECT_TRUE(da.init(_zkWrapper, _zkRoot));
    EXPECT_TRUE(da.loadTopicSchemas());
    EXPECT_EQ(0, da._topicSchemas.size());

    // read topic zk file fail
    ASSERT_EQ(EC_OK, fs::FileSystem::writeFile(_zkRoot + "/schema/topica", "xxx"));
    EXPECT_TRUE(da.loadTopicSchemas());
    EXPECT_EQ(0, da._topicSchemas.size());

    // ok
    SchemaInfos scmInfos;
    SchemaInfo *sc = scmInfos.add_sinfos();
    sc->set_version(100);
    sc->set_registertime(200);
    sc->set_schema("xxx");
    EXPECT_TRUE(da.writeTopicSchema("topica", scmInfos));
    EXPECT_TRUE(da.loadTopicSchemas());
    EXPECT_EQ(1, da._topicSchemas.size());
    const SchemaInfos &result = da._topicSchemas["topica"];
    EXPECT_EQ(1, result.sinfos_size());
    EXPECT_EQ(100, result.sinfos(0).version());
    EXPECT_EQ(200, result.sinfos(0).registertime());
    EXPECT_EQ("xxx", result.sinfos(0).schema());
}

TEST_F(AdminZkDataAccessorTest, testRemoveTopicSchema) {
    AdminZkDataAccessor da;
    EXPECT_TRUE(da.init(_zkWrapper, _zkRoot));
    int32_t removedVersion = 0;
    EXPECT_TRUE(da.addTopicSchema("topica", 400, "xxx", removedVersion, 2));

    // not exsit
    EXPECT_TRUE(da.removeTopicSchema("topicb"));
    EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/schema/topica"));
    EXPECT_EQ(1, da._topicSchemas.size());

    // remove fail
    _zkWrapper->close(); // remove zk path will fail
    EXPECT_TRUE(da.removeTopicSchema("topica"));
    _zkWrapper->open();
    EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/schema/topica"));
    EXPECT_EQ(1, da._topicSchemas.size());

    // sucess
    EXPECT_TRUE(da.removeTopicSchema("topica"));
    EXPECT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/schema/topica"));
    EXPECT_EQ(0, da._topicSchemas.size());
}

TEST_F(AdminZkDataAccessorTest, testDeleteTopic) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));

    TopicCreationRequest meta;
    meta.set_topicname("topica");
    meta.set_partitioncount(1);
    meta.set_needschema(true);
    ASSERT_TRUE(da.addTopic(meta));
    int32_t removedVersion = 0;
    EXPECT_TRUE(da.addTopicSchema("topica", 400, "xxx", removedVersion, 2));
    EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/schema/topica"));
    EXPECT_FALSE(da.deleteTopic({"topicb"}));
    EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/schema/topica"));

    // zk fail
    _zkWrapper->close();
    EXPECT_FALSE(da.deleteTopic({"topica"}));
    _zkWrapper->open();
    EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/schema/topica"));

    // sucess
    EXPECT_TRUE(da.deleteTopic({"topica"}));
    EXPECT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/schema/topica"));
    EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/schema"));
}

TEST_F(AdminZkDataAccessorTest, testWriteAndLoadTopicRWTime) {
    AdminZkDataAccessor da;
    EXPECT_TRUE(da.init(_zkWrapper, _zkRoot));
    EXPECT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/topic_rwinfo"));

    TopicRWInfos rwInfos;
    const size_t TOPIC_NUM = 10;
    for (size_t count = 0; count < TOPIC_NUM; ++count) {
        TopicRWInfo *info = rwInfos.add_infos();
        info->set_topicname(string("topic_") + std::to_string(count));
        info->set_lastwritetime(count);
        info->set_lastreadtime(count + 1);
    }

    EXPECT_TRUE(da.writeTopicRWTime(rwInfos));
    EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/topic_rwinfo"));
    rwInfos.clear_infos();
    EXPECT_EQ(0, rwInfos.infos_size());

    EXPECT_TRUE(da.loadTopicRWTime(rwInfos));
    EXPECT_EQ(TOPIC_NUM, rwInfos.infos_size());
    for (size_t count = 0; count < TOPIC_NUM; ++count) {
        const TopicRWInfo &info = rwInfos.infos(count);
        EXPECT_EQ(string("topic_") + std::to_string(count), info.topicname());
        EXPECT_EQ(count, info.lastwritetime());
        EXPECT_EQ(count + 1, info.lastreadtime());
    }
}

TEST_F(AdminZkDataAccessorTest, testWriteAndReadNoUseTopics) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));
    string noUseTopicDir(_zkRoot + "/nouse_topics");
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(noUseTopicDir));
    FileList fileList;
    ASSERT_EQ(EC_OK, fs::FileSystem::listDir(noUseTopicDir, fileList));
    ASSERT_EQ(0, fileList.size());

    const uint32_t maxAllowNum = 3;
    TopicCreationRequest meta;
    TopicMetas writeMetas, readMetas;
    string oldestFile;

    // add one topic
    fileList.clear();
    meta.set_topicname("topic1");
    meta.set_partitioncount(1);
    *writeMetas.add_topicmetas() = meta;
    ASSERT_TRUE(da.writeDeletedNoUseTopics(writeMetas, maxAllowNum));
    ASSERT_EQ(EC_OK, fs::FileSystem::listDir(noUseTopicDir, fileList));
    ASSERT_EQ(1, fileList.size());
    oldestFile = fileList[0];
    ASSERT_TRUE(da.loadLastDeletedNoUseTopics(readMetas));
    ASSERT_EQ(writeMetas.ShortDebugString(), readMetas.ShortDebugString());

    // add more topic
    fileList.clear();
    sleep(1);
    meta.set_topicname("topic2");
    *writeMetas.add_topicmetas() = meta;
    ASSERT_TRUE(da.writeDeletedNoUseTopics(writeMetas, maxAllowNum));
    ASSERT_EQ(EC_OK, fs::FileSystem::listDir(noUseTopicDir, fileList));
    ASSERT_EQ(2, fileList.size());
    ASSERT_TRUE(da.loadLastDeletedNoUseTopics(readMetas));
    ASSERT_EQ(writeMetas.ShortDebugString(), readMetas.ShortDebugString());

    // add more topic
    fileList.clear();
    sleep(1);
    meta.set_topicname("topic3");
    *writeMetas.add_topicmetas() = meta;
    ASSERT_TRUE(da.writeDeletedNoUseTopics(writeMetas, maxAllowNum));
    ASSERT_EQ(EC_OK, fs::FileSystem::listDir(noUseTopicDir, fileList));
    ASSERT_EQ(3, fileList.size());
    ASSERT_TRUE(da.loadLastDeletedNoUseTopics(readMetas));
    ASSERT_EQ(writeMetas.ShortDebugString(), readMetas.ShortDebugString());

    // write more, will delete oldest file
    fileList.clear();
    sleep(1);
    writeMetas.clear_topicmetas();
    *writeMetas.add_topicmetas() = meta;
    ASSERT_TRUE(da.writeDeletedNoUseTopics(writeMetas, maxAllowNum));
    ASSERT_EQ(EC_OK, fs::FileSystem::listDir(noUseTopicDir, fileList));
    ASSERT_EQ(3, fileList.size());
    ASSERT_TRUE(da.loadLastDeletedNoUseTopics(readMetas));
    for (size_t i = 0; i < fileList.size(); i++) {
        ASSERT_TRUE(oldestFile != fileList[i]);
    }
    ASSERT_EQ(writeMetas.ShortDebugString(), readMetas.ShortDebugString());

    // add more obsolete files
    fileList.clear();
    sleep(1);
    vector<string> topicFiles;
    ASSERT_EQ(EC_OK, fs::FileSystem::writeFile(noUseTopicDir + "/1633772160", "a"));
    ASSERT_EQ(EC_OK, fs::FileSystem::writeFile(noUseTopicDir + "/1633772161", "a"));
    ASSERT_EQ(EC_OK, fs::FileSystem::writeFile(noUseTopicDir + "/1633772162", "a"));
    ASSERT_EQ(EC_OK, fs::FileSystem::listDir(noUseTopicDir, fileList));
    ASSERT_TRUE(da.loadDeletedNoUseTopicFiles(topicFiles));
    ASSERT_EQ(6, fileList.size());
    ASSERT_EQ(6, topicFiles.size());
    sort(fileList.begin(), fileList.end());
    sort(topicFiles.begin(), topicFiles.end());
    for (size_t idx = 0; idx < 6; ++idx) {
        ASSERT_EQ(fileList[idx], topicFiles[idx]);
    }
    ASSERT_TRUE(da.writeDeletedNoUseTopics(writeMetas, maxAllowNum));
    fileList.clear();
    topicFiles.clear();
    ASSERT_EQ(EC_OK, fs::FileSystem::listDir(noUseTopicDir, fileList));
    ASSERT_TRUE(da.loadDeletedNoUseTopicFiles(topicFiles));
    ASSERT_EQ(3, fileList.size());
    ASSERT_EQ(3, topicFiles.size());
    std::sort(fileList.begin(), fileList.end());
    sort(topicFiles.begin(), topicFiles.end());
    ASSERT_TRUE(fileList[0] > "1633772162");
    for (size_t idx = 0; idx < 3; ++idx) {
        ASSERT_EQ(fileList[idx], topicFiles[idx]);
    }
}

void assert_topicmeta(const TopicCreationRequest &meta,
                      const TopicPartitionInfo &pinfo,
                      const string &topicName,
                      const string &topicGroup,
                      int partCnt,
                      int64_t expireTime,
                      int64_t reseveFileCnt,
                      bool enableTtl,
                      int64_t modifytime,
                      bool sealed,
                      PartitionStatus status,
                      string address = "") {
    ASSERT_EQ(topicName, meta.topicname());
    ASSERT_EQ(topicGroup, meta.topicgroup());
    ASSERT_EQ(partCnt, meta.partitioncount());
    ASSERT_EQ(expireTime, meta.topicexpiredtime());
    ASSERT_EQ(modifytime, meta.modifytime());
    ASSERT_EQ(reseveFileCnt, meta.reservedfilecount());
    ASSERT_EQ(enableTtl, meta.enablettldel());
    ASSERT_EQ(sealed, meta.sealed());
    ASSERT_EQ(partCnt, pinfo.partitioninfos_size());
    for (int i = 0; i < partCnt; ++i) {
        const PartitionInfo &pi = pinfo.partitioninfos(i);
        ASSERT_EQ(uint32_t(i), pi.id());
        ASSERT_EQ(address, pi.brokeraddress());
        ASSERT_EQ(status, pi.status());
    }
}

TEST_F(AdminZkDataAccessorTest, testAddPhysicTopic) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));
    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/topic_meta"));
    int64_t brokerCfgTTlSec = 3600;
    TopicCreationRequest retLMeta;
    TopicCreationRequest retLastPMeta;
    TopicCreationRequest retNewPMeta;

    TopicCreationRequest request;
    request.set_topicname("topic_1");
    request.set_partitioncount(1);
    request.set_topictype(TOPIC_TYPE_PHYSIC);
    request.set_topicgroup("group");
    request.set_topicexpiredtime(100);
    request.set_sealed(true);
    ASSERT_FALSE(da.addPhysicTopic(request, brokerCfgTTlSec, retLMeta, retLastPMeta, retNewPMeta));
    request.set_topictype(TOPIC_TYPE_NORMAL);
    ASSERT_FALSE(da.addPhysicTopic(request, brokerCfgTTlSec, retLMeta, retLastPMeta, retNewPMeta));
    // not found
    request.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    ASSERT_FALSE(da.addPhysicTopic(request, brokerCfgTTlSec, retLMeta, retLastPMeta, retNewPMeta));
    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/topic_meta"));
    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/partition_info"));

    // success
    int64_t startTime = TimeUtility::currentTime();
    // topic_1 (1)
    ASSERT_TRUE(da.addTopic(request));
    // topic_1-ts-1
    ASSERT_TRUE(da.addPhysicTopic(request, brokerCfgTTlSec, retLMeta, retLastPMeta, retNewPMeta));
    int64_t topic1p1Mt = da._topicMetas.topicmetas(0).modifytime();
    int64_t topic1Et = da._topicMetas.topicmetas(0).modifytime() / 1000000 + brokerCfgTTlSec;

    ASSERT_EQ(2, da._topicMetas.topicmetas_size());
    auto physic = da._topicMetas.topicmetas(1);
    auto topic1p1 = physic.topicname();
    int64_t endTime = TimeUtility::currentTime();
    vector<string> nameVec = StringUtil::split(topic1p1, "-");
    ASSERT_EQ(3, nameVec.size());
    ASSERT_EQ("topic_1", nameVec[0]);
    int64_t ptime;
    ASSERT_TRUE(StringUtil::strToInt64(nameVec[1].c_str(), ptime));
    ASSERT_TRUE(ptime >= startTime);
    ASSERT_TRUE(ptime <= endTime);
    ASSERT_EQ("1", nameVec[2]);
    ASSERT_EQ(TOPIC_TYPE_PHYSIC, physic.topictype());
    ASSERT_EQ(-1, physic.topicexpiredtime());
    ASSERT_EQ(-1, physic.reservedfilecount());
    ASSERT_FALSE(physic.enablettldel());
    ASSERT_FALSE(physic.sealed());
    ASSERT_EQ("group", physic.topicgroup());
    auto logic = da._topicMetas.topicmetas(0);
    int64_t topic1modify = logic.modifytime();
    ASSERT_EQ(1, logic.physictopiclst_size());
    ASSERT_EQ(physic.topicname(), logic.physictopiclst(0));
    ASSERT_EQ(ptime, topic1modify);
    ASSERT_EQ(ptime / 1000000 + brokerCfgTTlSec, logic.topicexpiredtime());
    ASSERT_EQ(-1, logic.reservedfilecount());
    ASSERT_TRUE(logic.sealed());

    // add
    request.set_topicname("topic_2");
    request.set_topictype(TOPIC_TYPE_LOGIC);
    request.set_sealed(false);
    string topic2p1 = "topic_2-1641890784000000-2";
    *request.add_physictopiclst() = topic2p1;
    // topic_2 (1)
    ASSERT_TRUE(da.addTopic(request));
    request.set_topicname(topic2p1);
    request.set_topictype(TOPIC_TYPE_PHYSIC);
    request.set_partitioncount(2);
    request.set_createtime(1641890784000000);
    request.set_modifytime(1641890784000000);
    // topic_2-1641890784000000-2
    ASSERT_TRUE(da.addTopic(request));

    // not found
    request.set_topicname("topic_3");
    request.set_topictype(TOPIC_TYPE_LOGIC);
    ASSERT_FALSE(da.addPhysicTopic(request, brokerCfgTTlSec, retLMeta, retLastPMeta, retNewPMeta));

    // sucess
    request.set_topicname("topic_2");
    request.set_topictype(TOPIC_TYPE_LOGIC);
    request.set_partitioncount(3);
    // topic_2-ts-3
    ASSERT_TRUE(da.addPhysicTopic(request, brokerCfgTTlSec, retLMeta, retLastPMeta, retNewPMeta));
    int64_t topic2p2Mt = da._topicMetas.topicmetas(2).modifytime();
    int64_t topic2p1Et = da._topicMetas.topicmetas(2).modifytime() / 1000000 + brokerCfgTTlSec;

    ASSERT_EQ(5, da._topicMetas.topicmetas_size());
    physic = da._topicMetas.topicmetas(4);
    string topic2p2 = physic.topicname();
    endTime = TimeUtility::currentTime();
    nameVec = StringUtil::split(topic2p2, "-");
    ASSERT_EQ(3, nameVec.size());
    ASSERT_EQ("topic_2", nameVec[0]);
    ASSERT_TRUE(StringUtil::strToInt64(nameVec[1].c_str(), ptime));
    ASSERT_TRUE(ptime >= startTime);
    ASSERT_TRUE(ptime <= endTime);
    ASSERT_EQ("3", nameVec[2]);
    ASSERT_EQ(TOPIC_TYPE_PHYSIC, physic.topictype());
    ASSERT_EQ(-1, physic.topicexpiredtime());
    ASSERT_EQ(-1, physic.reservedfilecount());
    ASSERT_FALSE(physic.enablettldel());
    ASSERT_EQ("group", physic.topicgroup());
    logic = da._topicMetas.topicmetas(2);
    ASSERT_EQ(2, logic.physictopiclst_size());
    ASSERT_EQ(topic2p1, logic.physictopiclst(0));
    ASSERT_EQ(physic.topicname(), logic.physictopiclst(1));
    auto lastPhysic = da._topicMetas.topicmetas(3);
    int64_t topic2p1modify = lastPhysic.modifytime();
    ASSERT_EQ(ptime, topic2p1modify);
    ASSERT_EQ(ptime / 1000000 + brokerCfgTTlSec, lastPhysic.topicexpiredtime());
    ASSERT_EQ(-1, lastPhysic.reservedfilecount());

    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/topic_meta"));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/partition_info"));

    AdminZkDataAccessor da2;
    ASSERT_TRUE(da2.init(_zkWrapper, _zkRoot));

    TopicMetas topicMetas;
    TopicPartitionInfos topicPartInfos;
    int TOPIC_NUM = 5;
    ASSERT_TRUE(da2.loadTopicInfos(topicMetas, topicPartInfos));
    ASSERT_EQ(TOPIC_NUM, topicMetas.topicmetas_size());
    ASSERT_EQ(TOPIC_NUM, topicPartInfos.topicpartitioninfos_size());

    ASSERT_EQ("topic_1", topicMetas.topicmetas(0).topicname());
    ASSERT_EQ(topic1p1, topicMetas.topicmetas(1).topicname());
    ASSERT_EQ("topic_2", topicMetas.topicmetas(2).topicname());
    ASSERT_EQ(topic2p1, topicMetas.topicmetas(3).topicname());
    ASSERT_EQ(topic2p2, topicMetas.topicmetas(4).topicname());

    ASSERT_NO_FATAL_FAILURE(assert_topicmeta(topicMetas.topicmetas(0),
                                             topicPartInfos.topicpartitioninfos(0),
                                             "topic_1",
                                             "group",
                                             1,
                                             topic1Et,
                                             -1,
                                             true,
                                             topic1p1Mt,
                                             true,
                                             PARTITION_STATUS_NONE));
    ASSERT_NO_FATAL_FAILURE(assert_topicmeta(topicMetas.topicmetas(1),
                                             topicPartInfos.topicpartitioninfos(1),
                                             topic1p1,
                                             "group",
                                             1,
                                             -1,
                                             -1,
                                             false,
                                             topic1p1Mt,
                                             false,
                                             PARTITION_STATUS_NONE));
    ASSERT_NO_FATAL_FAILURE(assert_topicmeta(topicMetas.topicmetas(2),
                                             topicPartInfos.topicpartitioninfos(2),
                                             "topic_2",
                                             "group",
                                             3,
                                             100,
                                             -1,
                                             true,
                                             topic2p2Mt,
                                             false,
                                             PARTITION_STATUS_RUNNING));
    ASSERT_NO_FATAL_FAILURE(assert_topicmeta(topicMetas.topicmetas(3),
                                             topicPartInfos.topicpartitioninfos(3),
                                             topic2p1,
                                             "group",
                                             2,
                                             topic2p1Et,
                                             -1,
                                             true,
                                             topic2p2Mt,
                                             false,
                                             PARTITION_STATUS_NONE));
    ASSERT_NO_FATAL_FAILURE(assert_topicmeta(topicMetas.topicmetas(4),
                                             topicPartInfos.topicpartitioninfos(4),
                                             topic2p2,
                                             "group",
                                             3,
                                             -1,
                                             -1,
                                             false,
                                             topic2p2Mt,
                                             false,
                                             PARTITION_STATUS_NONE));
}

TEST_F(AdminZkDataAccessorTest, testDeleteTopicsAllType) {
    AdminZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkWrapper, _zkRoot));

    map<string, TopicCreationRequest> todelTopicMetas;
    TopicCreationRequest meta;
    TopicPartitionInfo pinfo;
    // N, delete directory
    meta.set_topicname("topic_1");
    meta.set_topictype(TOPIC_TYPE_NORMAL);
    meta.set_partitioncount(2);
    todelTopicMetas["topic_1"] = meta; // delete
    da.addTopic(meta);

    // P has no logic in zk, will not delete
    meta.set_topicname("topic_2-2123-4");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    todelTopicMetas["topic_2-2123-4"] = meta;
    da.addTopic(meta);

    // L has one child in zk meta, will not delete
    meta.set_topicname("topic_3");
    meta.set_topictype(TOPIC_TYPE_LOGIC);
    *meta.add_physictopiclst() = "topic_3-3123-5";
    *meta.add_physictopiclst() = "topic_3-3456-6";
    da.addTopic(meta);
    // only add one physic
    meta.set_topicname("topic_3-3123-5");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    da.addTopic(meta);
    todelTopicMetas["topic_3-3123-5"] = meta;

    // LP has no child, will not delete
    meta.clear_physictopiclst();
    meta.set_topicname("topic_4");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    todelTopicMetas["topic_4"] = meta; // no child, ignore
    da.addTopic(meta);

    // LP has one child, will update
    meta.clear_physictopiclst();
    meta.set_topicname("topic_5");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    *meta.add_physictopiclst() = "topic_5-5123-7";
    da.addTopic(meta);
    todelTopicMetas["topic_5"] = meta;
    meta.set_topicname("topic_5-5123-7");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    da.addTopic(meta);

    // L, delete first child success
    meta.clear_physictopiclst();
    meta.set_topicname("topic_6");
    meta.set_topictype(TOPIC_TYPE_LOGIC);
    *meta.add_physictopiclst() = "topic_6-6123-5";
    *meta.add_physictopiclst() = "topic_6-6456-6";
    da.addTopic(meta);
    meta.set_topicname("topic_6-6123-5");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    todelTopicMetas["topic_6-6123-5"] = meta;
    da.addTopic(meta);
    meta.set_topicname("topic_6-6456-6");
    da.addTopic(meta);

    // L, delete last child will fail
    meta.clear_physictopiclst();
    meta.set_topicname("topic_7");
    meta.set_topictype(TOPIC_TYPE_LOGIC);
    *meta.add_physictopiclst() = "topic_7-7123-5";
    *meta.add_physictopiclst() = "topic_7-7456-6";
    da.addTopic(meta);
    meta.set_topicname("topic_7-7123-5");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    da.addTopic(meta);
    meta.set_topicname("topic_7-7456-6");
    da.addTopic(meta);
    todelTopicMetas["topic_7-456-6"] = meta;

    // L, only child cannot delete
    meta.clear_physictopiclst();
    meta.set_topicname("topic_8");
    meta.set_topictype(TOPIC_TYPE_LOGIC);
    *meta.add_physictopiclst() = "topic_8-8123-5";
    da.addTopic(meta);
    meta.set_topicname("topic_8-8123-5");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    da.addTopic(meta);
    todelTopicMetas["topic_8-8123-5"] = meta;

    vector<TopicCreationRequest> updateTopicMetasVec;
    set<string> deleteTopics;
    ASSERT_TRUE(da.deleteTopicsAllType(todelTopicMetas, deleteTopics, updateTopicMetasVec));
    map<string, TopicCreationRequest> updateTopicMetas;
    for (auto &meta : updateTopicMetasVec) {
        updateTopicMetas[meta.topicname()] = meta;
    }

    ASSERT_EQ(2, deleteTopics.size());
    ASSERT_TRUE(deleteTopics.end() != deleteTopics.find("topic_1"));
    ASSERT_TRUE(deleteTopics.end() != deleteTopics.find("topic_6-6123-5"));

    ASSERT_EQ(4, updateTopicMetas.size());
    auto topic5iter = updateTopicMetas.find("topic_5");
    ASSERT_TRUE(updateTopicMetas.end() != topic5iter);
    ASSERT_EQ(TOPIC_TYPE_LOGIC, topic5iter->second.topictype());
    ASSERT_EQ(-1, topic5iter->second.topicexpiredtime());
    ASSERT_EQ(-1, topic5iter->second.reservedfilecount());
    ASSERT_FALSE(topic5iter->second.sealed());
    ASSERT_EQ(1, topic5iter->second.physictopiclst_size());
    ASSERT_EQ("topic_5-5123-7", topic5iter->second.physictopiclst(0));

    auto topic5Piter = updateTopicMetas.find("topic_5-5123-7");
    ASSERT_TRUE(updateTopicMetas.end() != topic5Piter);
    ASSERT_FALSE(topic5Piter->second.sealed());
    ASSERT_EQ(TOPIC_TYPE_PHYSIC, topic5Piter->second.topictype());
    ASSERT_TRUE(topic5Piter->second.enablettldel());

    auto topic6iter = updateTopicMetas.find("topic_6");
    ASSERT_TRUE(updateTopicMetas.end() != topic6iter);
    ASSERT_EQ(TOPIC_TYPE_LOGIC, topic6iter->second.topictype());
    ASSERT_EQ(1, topic6iter->second.physictopiclst_size());
    ASSERT_EQ("topic_6-6456-6", topic6iter->second.physictopiclst(0));

    auto topic6Piter = updateTopicMetas.find("topic_6-6456-6");
    ASSERT_TRUE(updateTopicMetas.end() != topic6Piter);
    ASSERT_FALSE(topic6Piter->second.sealed());
    ASSERT_EQ(TOPIC_TYPE_PHYSIC, topic6Piter->second.topictype());
    ASSERT_TRUE(topic6Piter->second.enablettldel());

    ASSERT_EQ(13, da._topicMetas.topicmetas_size());
    ASSERT_EQ(13, da._topicPartitionInfos.topicpartitioninfos_size());
    ASSERT_EQ(da._topicMetas.topicmetas(0).topicname(), "topic_2-2123-4");
    ASSERT_EQ(da._topicMetas.topicmetas(1).topicname(), "topic_3");
    ASSERT_EQ(da._topicMetas.topicmetas(2).topicname(), "topic_3-3123-5");
    ASSERT_EQ(da._topicMetas.topicmetas(3).topicname(), "topic_4");
    ASSERT_EQ(da._topicMetas.topicmetas(4).topicname(), "topic_5");
    ASSERT_EQ(da._topicMetas.topicmetas(5).topicname(), "topic_5-5123-7");
    ASSERT_EQ(da._topicMetas.topicmetas(6).topicname(), "topic_6");
    ASSERT_EQ(da._topicMetas.topicmetas(7).topicname(), "topic_6-6456-6");
    ASSERT_EQ(da._topicMetas.topicmetas(8).topicname(), "topic_7");
    ASSERT_EQ(da._topicMetas.topicmetas(9).topicname(), "topic_7-7123-5");
    ASSERT_EQ(da._topicMetas.topicmetas(10).topicname(), "topic_7-7456-6");
    ASSERT_EQ(da._topicMetas.topicmetas(11).topicname(), "topic_8");
    ASSERT_EQ(da._topicMetas.topicmetas(12).topicname(), "topic_8-8123-5");

    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(0).topicname(), "topic_2-2123-4");
    ASSERT_EQ(PARTITION_STATUS_NONE, da._topicPartitionInfos.topicpartitioninfos(0).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(1).topicname(), "topic_3");
    ASSERT_EQ(PARTITION_STATUS_RUNNING, da._topicPartitionInfos.topicpartitioninfos(1).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(2).topicname(), "topic_3-3123-5");
    ASSERT_EQ(PARTITION_STATUS_NONE, da._topicPartitionInfos.topicpartitioninfos(2).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(3).topicname(), "topic_4");
    ASSERT_EQ(PARTITION_STATUS_NONE, da._topicPartitionInfos.topicpartitioninfos(3).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(4).topicname(), "topic_5");
    ASSERT_EQ(PARTITION_STATUS_RUNNING, da._topicPartitionInfos.topicpartitioninfos(4).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(5).topicname(), "topic_5-5123-7");
    ASSERT_EQ(PARTITION_STATUS_NONE, da._topicPartitionInfos.topicpartitioninfos(5).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(6).topicname(), "topic_6");
    ASSERT_EQ(PARTITION_STATUS_RUNNING, da._topicPartitionInfos.topicpartitioninfos(6).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(7).topicname(), "topic_6-6456-6");
    ASSERT_EQ(PARTITION_STATUS_NONE, da._topicPartitionInfos.topicpartitioninfos(7).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(8).topicname(), "topic_7");
    ASSERT_EQ(PARTITION_STATUS_RUNNING, da._topicPartitionInfos.topicpartitioninfos(8).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(9).topicname(), "topic_7-7123-5");
    ASSERT_EQ(PARTITION_STATUS_NONE, da._topicPartitionInfos.topicpartitioninfos(9).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(10).topicname(), "topic_7-7456-6");
    ASSERT_EQ(PARTITION_STATUS_NONE, da._topicPartitionInfos.topicpartitioninfos(10).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(11).topicname(), "topic_8");
    ASSERT_EQ(PARTITION_STATUS_RUNNING, da._topicPartitionInfos.topicpartitioninfos(11).partitioninfos(0).status());
    ASSERT_EQ(da._topicPartitionInfos.topicpartitioninfos(12).topicname(), "topic_8-8123-5");
    ASSERT_EQ(PARTITION_STATUS_NONE, da._topicPartitionInfos.topicpartitioninfos(12).partitioninfos(0).status());
}

TEST_F(AdminZkDataAccessorTest, testLoadSendChangePartCntTask) {
    AdminZkDataAccessor da;
    // zk not start
    EXPECT_FALSE(da.loadChangePartCntTask());

    // file not exist
    EXPECT_TRUE(da.init(_zkWrapper, _zkRoot));
    EXPECT_TRUE(da.loadChangePartCntTask());
    EXPECT_EQ(0, da._changePartCntTasks.tasks_size());
    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(_zkRoot + "/change_partcnt_tasks"));

    // file invalid
    ASSERT_EQ(EC_OK, fs::FileSystem::writeFile(_zkRoot + "/change_partcnt_tasks", "xxx"));
    EXPECT_FALSE(da.loadChangePartCntTask());
    EXPECT_EQ(0, da._changePartCntTasks.tasks_size());

    // send task
    TopicCreationRequest meta;
    meta.set_topicname("topic_1");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    meta.set_partitioncount(1);
    meta.set_modifytime(1);
    ASSERT_EQ(ERROR_NONE, da.sendChangePartCntTask(meta));
    ASSERT_EQ(EC_TRUE, fs::FileSystem::isExist(_zkRoot + "/change_partcnt_tasks"));
    ASSERT_EQ(1, da._changePartCntTasks.tasks_size());

    meta.set_modifytime(2);
    meta.set_partitioncount(2);
    ASSERT_EQ(ERROR_ADMIN_CHG_PART_TASK_NOT_FINISH, da.sendChangePartCntTask(meta));
    ASSERT_EQ(1, da._changePartCntTasks.tasks_size()); // same topic task will ignore

    meta.set_topicname("topic_2");
    ASSERT_EQ(ERROR_NONE, da.sendChangePartCntTask(meta));
    ASSERT_EQ("topic_1", da._changePartCntTasks.tasks(0).meta().topicname());
    ASSERT_EQ(1, da._changePartCntTasks.tasks(0).meta().partitioncount());
    ASSERT_EQ(1, da._changePartCntTasks.tasks(0).taskid());
    ASSERT_EQ("topic_2", da._changePartCntTasks.tasks(1).meta().topicname());
    ASSERT_EQ(2, da._changePartCntTasks.tasks(1).meta().partitioncount());
    ASSERT_EQ(2, da._changePartCntTasks.tasks(1).taskid());
}

TEST_F(AdminZkDataAccessorTest, testUpdateChangePartCntTasks) {
    AdminZkDataAccessor da;
    EXPECT_TRUE(da.init(_zkWrapper, _zkRoot));
    TopicCreationRequest meta;
    EXPECT_EQ(0, da.getChangePartCntTasks().tasks_size());

    da.updateChangePartCntTasks({});
    EXPECT_EQ(0, da.getChangePartCntTasks().tasks_size());

    da.updateChangePartCntTasks({1, 3});
    EXPECT_EQ(0, da.getChangePartCntTasks().tasks_size());

    for (int i = 1; i <= 3; ++i) {
        meta.set_topicname("topic_" + to_string(i));
        meta.set_partitioncount(i);
        meta.set_modifytime(i);
        ASSERT_EQ(ERROR_NONE, da.sendChangePartCntTask(meta));
    }

    da.updateChangePartCntTasks({});
    EXPECT_EQ(3, da.getChangePartCntTasks().tasks_size());

    da.updateChangePartCntTasks({1, 3});
    EXPECT_EQ(1, da.getChangePartCntTasks().tasks_size());
    EXPECT_EQ(2, da.getChangePartCntTasks().tasks(0).taskid());
}

TEST_F(AdminZkDataAccessorTest, testTmp) {
    TopicPartitionInfos topicPartitionInfos;
    auto *tpi = topicPartitionInfos.add_topicpartitioninfos();
    tpi->set_topicname("test");
    auto *pi = tpi->add_partitioninfos();
    pi->set_id(0);
    pi->set_status(PARTITION_STATUS_NONE);
    pi = tpi->add_partitioninfos();
    pi->set_id(1);
    pi->set_status(PARTITION_STATUS_WAITING);

    tpi = topicPartitionInfos.add_topicpartitioninfos();
    tpi->set_topicname("test2");
    pi = tpi->add_partitioninfos();
    pi->set_id(0);
    pi->set_status(PARTITION_STATUS_RUNNING);

    cout << "before: " << topicPartitionInfos.ShortDebugString() << endl;

    auto *logicPinfo = topicPartitionInfos.mutable_topicpartitioninfos(1);
    logicPinfo->clear_partitioninfos();
    for (uint32_t i = 0; i < 3; ++i) {
        PartitionInfo *pi = logicPinfo->add_partitioninfos();
        pi->set_id(i);
        pi->set_status(PARTITION_STATUS_STARTING);
    }
    cout << "after: " << topicPartitionInfos.ShortDebugString() << endl;
}

TEST_F(AdminZkDataAccessorTest, testReadMasterVersion) {
    AdminZkDataAccessor da;
    EXPECT_TRUE(da.init(_zkWrapper, _zkRoot));
    auto path = common::PathDefine::getSelfMasterVersionFile(_zkRoot);
    EXPECT_EQ(EC_FALSE, fs::FileSystem::isExist(path));

    EXPECT_TRUE(da.writeMasterVersion(123));
    EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(path));
    std::string content;
    EXPECT_EQ(EC_OK, fs::FileSystem::readFile(path, content));
    EXPECT_EQ("123", content);
}

TEST_F(AdminZkDataAccessorTest, testWriteMasterVersion) {
    AdminZkDataAccessor da;
    EXPECT_TRUE(da.init(_zkWrapper, _zkRoot));
    auto path = common::PathDefine::getSelfMasterVersionFile(_zkRoot);

    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(path));
    uint64_t masterVersion = 0;
    ASSERT_TRUE(da.readMasterVersion(masterVersion));
    EXPECT_EQ(0, masterVersion);

    EXPECT_TRUE(da.writeMasterVersion(123));
    EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(path));
    ASSERT_TRUE(da.readMasterVersion(masterVersion));
    ASSERT_EQ(123, masterVersion);
}

TEST_F(AdminZkDataAccessorTest, testLoadCleanAtDeleteTopicTasks) {
    AdminZkDataAccessor da;
    EXPECT_TRUE(da.init(_zkWrapper, _zkRoot));
    CleanAtDeleteTopicTasks tasks;
    EXPECT_TRUE(da.loadCleanAtDeleteTopicTasks(tasks));
    EXPECT_EQ(0, tasks.tasks_size());

    CleanAtDeleteTopicTask *task = tasks.add_tasks();
    task->set_topicname("cad_topic1");
    vector<string> dfsPaths = {"dfs://swift1/root1/", "dfs://swift2/root2/"};
    *(task->mutable_dfspath()) = {dfsPaths.begin(), dfsPaths.end()};
    CleanAtDeleteTopicTask *task2 = tasks.add_tasks();
    task2->set_topicname("cad_topic2");
    vector<string> dfsPath2 = {"pangu://pov/format/", "pangu://casually/", "pangu://789"};
    *(task2->mutable_dfspath()) = {dfsPath2.begin(), dfsPath2.end()};
    const string &path = StringUtil::formatString("%s/clean_at_delete_tasks", _zkRoot.c_str());
    string content;
    tasks.SerializeToString(&content);
    autil::ZlibCompressor compressor(Z_BEST_COMPRESSION);
    std::string compressedData;
    EXPECT_TRUE(compressor.compress(content, compressedData));
    content = compressedData;
    EXPECT_EQ(fslib::EC_OK, FileSystem::writeFile(path, content));

    CleanAtDeleteTopicTasks remoteTasks;
    EXPECT_TRUE(da.loadCleanAtDeleteTopicTasks(remoteTasks));
    EXPECT_EQ(2, remoteTasks.tasks_size());
    EXPECT_EQ("cad_topic1", remoteTasks.tasks(0).topicname());
    EXPECT_EQ(2, remoteTasks.tasks(0).dfspath_size());
    EXPECT_EQ("dfs://swift1/root1/", remoteTasks.tasks(0).dfspath(0));
    EXPECT_EQ("dfs://swift2/root2/", remoteTasks.tasks(0).dfspath(1));
    EXPECT_EQ("cad_topic2", remoteTasks.tasks(1).topicname());
    EXPECT_EQ(3, remoteTasks.tasks(1).dfspath_size());
    EXPECT_EQ("pangu://pov/format/", remoteTasks.tasks(1).dfspath(0));
    EXPECT_EQ("pangu://casually/", remoteTasks.tasks(1).dfspath(1));
    EXPECT_EQ("pangu://789", remoteTasks.tasks(1).dfspath(2));
    CleanAtDeleteTopicTasks emptyTasks;
    da.serializeCleanAtDeleteTasks(emptyTasks);
    EXPECT_EQ(fslib::EC_OK, FileSystem::readFile(path, content));
    remoteTasks.ParseFromString(content);
    EXPECT_EQ(0, remoteTasks.tasks_size());
}

TEST_F(AdminZkDataAccessorTest, testRemoveOldWriterVersionData) {
    AdminZkDataAccessor da;
    EXPECT_TRUE(da.init(_zkWrapper, _zkRoot));
    auto path = _zkRoot + "/write_version_control";
    ASSERT_EQ(EC_FALSE, fs::FileSystem::isExist(path));
    EXPECT_EQ(EC_OK, fs::FileSystem::mkDir(path));
    for (int index = 0; index < 5; ++index) {
        EXPECT_EQ(EC_OK, fs::FileSystem::mkDir(path + "/topic_" + to_string(index)));
    }
    FileList fileLst;
    EXPECT_EQ(EC_OK, fs::FileSystem::listDir(path, fileLst));
    EXPECT_EQ(5, fileLst.size());

    set<string> existTopics;
    existTopics.insert("topic_2");
    existTopics.insert("topic_3");
    existTopics.insert("topic_5");

    // 1. topic_0, topic_1, topic_4 will remove
    da.removeOldWriterVersionData(existTopics);
    fileLst.clear();
    EXPECT_EQ(EC_OK, fs::FileSystem::listDir(path, fileLst));
    EXPECT_EQ(2, fileLst.size());
    std::sort(fileLst.begin(), fileLst.end());
    EXPECT_EQ("topic_2", fileLst[0]);
    EXPECT_EQ("topic_3", fileLst[1]);

    // 2. empty topic, not remove
    existTopics.clear();
    da.removeOldWriterVersionData(existTopics);
    fileLst.clear();
    EXPECT_EQ(EC_OK, fs::FileSystem::listDir(path, fileLst));
    EXPECT_EQ(2, fileLst.size());

    // 3. remove all
    existTopics.insert("topic_6");
    da.removeOldWriterVersionData(existTopics);
    fileLst.clear();
    EXPECT_EQ(EC_OK, fs::FileSystem::listDir(path, fileLst));
    EXPECT_EQ(0, fileLst.size());
}

} // namespace admin
} // namespace swift
