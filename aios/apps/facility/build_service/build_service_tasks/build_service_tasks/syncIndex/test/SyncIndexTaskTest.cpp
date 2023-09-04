#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service_tasks/channel/test/MockRpcChannel.h"
#include "fslib/util/FileUtil.h"
// Workaround for test private/protected member
#include "build_service_tasks/syncIndex/SyncIndexTask.h"
#include "build_service_tasks/test/unittest.h"
#include "google/protobuf/util/json_util.h"
#include "worker_framework/LeaderInfo.h"

using namespace std;
using namespace testing;
using namespace build_service::io;
using namespace build_service::util;
using namespace build_service::config;
using namespace autil::legacy;
using namespace autil;
using namespace arpc;

namespace build_service { namespace task_base {

class SyncIndexTaskTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();
    bool doGetStatus(const ::madrox::proto::GetStatusRequest& request,
                     ::madrox::proto::GetStatusResponse& response) const
    {
        string madroxInJson;
        if (request.deploymeta().size() == 1) {
            madroxInJson = getFailedMadroxInfo();
        }
        if (request.deploymeta().size() == 2) {
            madroxInJson = getNormalMadroxInfo();
        }
        auto status1 = google::protobuf::util::JsonStringToMessage(madroxInJson, &response);
        BS_LOG(INFO, "request [%s], response [%s]", request.ShortDebugString().c_str(),
               response.ShortDebugString().c_str());
        return true;
    }

private:
    string getFailedMadroxInfo() const;
    string getCancelMadroxInfo() const;
    string getNormalMadroxInfo() const;

protected:
    proto::PartitionId _pid;
    std::shared_ptr<MockMadroxChannel> _mockMadroxChannel;
    std::unique_ptr<SyncIndexTask> _syncIndexTask;
    proto::InformResponse _response;
    string _indexRoot;
};

void SyncIndexTaskTest::setUp()
{
    _syncIndexTask = std::make_unique<SyncIndexTask>();
    _mockMadroxChannel = make_shared<MockMadroxChannel>();
    _pid = proto::PartitionId();
    _pid.set_role(proto::ROLE_TASK);
    _pid.set_step(proto::BUILD_STEP_INC);
    _pid.mutable_range()->set_from(0);
    _pid.mutable_range()->set_to(65535);
    _pid.add_clusternames("allCluster");
    _pid.mutable_buildid()->set_appname("testapp");
    _pid.mutable_buildid()->set_datatable("simple");
    _pid.mutable_buildid()->set_generationid(1234567);
    _syncIndexTask->_madroxChannel = _mockMadroxChannel;

    string jsonStr;
    fslib::util::FileUtil::readFile(GET_TEST_DATA_PATH() + "/sync_index_test/config/build_app.json.template", jsonStr);
    const auto& tempDataPath = GET_TEMP_DATA_PATH();
    fslib::util::FileUtil::atomicCopy(GET_TEST_DATA_PATH() + "/sync_index_test/config",
                                      tempDataPath + "/sync_index_test/config");
    BuildServiceConfig BSConfig;
    FromJsonString(BSConfig, jsonStr);
    BSConfig.setIndexRoot(tempDataPath);
    _indexRoot = tempDataPath;
    jsonStr = ToJsonString(BSConfig);
    fslib::util::FileUtil::writeFile(tempDataPath + "/sync_index_test/config/build_app.json", jsonStr);
}

void SyncIndexTaskTest::tearDown()
{
    _syncIndexTask.reset();
    _mockMadroxChannel.reset();
}
TEST_F(SyncIndexTaskTest, testCheckTaget)
{
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/sync_index_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "cluster1";

    initParam.buildId.appName = _pid.buildid().appname();
    initParam.buildId.dataTable = _pid.buildid().datatable();
    initParam.buildId.generationId = _pid.buildid().generationid();
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 1;
    ASSERT_TRUE(_syncIndexTask->init(initParam));

    config::TaskTarget taskTarget;

    vector<versionid_t> targetVersions;

    // no madrox root and version
    ASSERT_FALSE(_syncIndexTask->checkTarget(taskTarget.getTargetDescription(), targetVersions));

    // false, empty madrox admin addr
    taskTarget.addTargetDescription(MADROX_ADMIN_ADDRESS, "");
    ASSERT_FALSE(_syncIndexTask->checkTarget(taskTarget.getTargetDescription(), targetVersions));

    // false, empty target version
    vector<versionid_t> versions;
    taskTarget.addTargetDescription("versions", ToJsonString(versions));
    ASSERT_FALSE(_syncIndexTask->checkTarget(taskTarget.getTargetDescription(), targetVersions));

    // false, invalid version
    versions.push_back(INVALID_VERSION);
    taskTarget.updateTargetDescription("versions", ToJsonString(versions));
    ASSERT_FALSE(_syncIndexTask->checkTarget(taskTarget.getTargetDescription(), targetVersions));

    // true
    versions = vector<versionid_t> {3, 4};
    taskTarget.updateTargetDescription(MADROX_ADMIN_ADDRESS, "zfs://madrox_admin");
    taskTarget.updateTargetDescription("versions", ToJsonString(versions));
    taskTarget.updateTargetDescription("sync_index_root", "dfs://xxx");
    ASSERT_TRUE(_syncIndexTask->checkTarget(taskTarget.getTargetDescription(), targetVersions));
}

TEST_F(SyncIndexTaskTest, testInit)
{
    config::TaskTarget taskTarget;
    Task::TaskInitParam taskInitParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/sync_index_test/config/"));
    taskInitParam.resourceReader = resourceReader;
    ASSERT_FALSE(_syncIndexTask->init(taskInitParam));
    ASSERT_FALSE(_syncIndexTask->isDone(taskTarget));

    std::string clusterName = "cluster1";
    taskInitParam.clusterName = clusterName;
    ASSERT_TRUE(_syncIndexTask->init(taskInitParam));
    ASSERT_FALSE(_syncIndexTask->isDone(taskTarget));
}

std::string SyncIndexTaskTest::getNormalMadroxInfo() const
{
    string madroxInJson = R"(
    {
        "dest" :
        [
            {
                "destRoot" : "hdfs://abx/",
                "finishedItemCount" : 1000,
                "failedItemCount" : 0,
                "totalItemCount" : 10,
                "finishTimestamp" : 2000,
                "itemDetail" : [],
                "canceledCount" : 0,
                "pendingRequests" : 0,
                "lastUpdateTimestamp" : 2000,
                "initTimestamp" : 1000,
                "locked" : false,
                "destHash" : 0,
                "configId" : 0,
                "deployMetaDescription" : "minVersion=1;maxVersion=3;partitionCount=32;metaCount=64",
                "deployMetaDetail":
                 [
                     {
                         "path":
                             "partition_0_65535\/deploy_meta.1",
                         "status":
                             "DONE"
                     },
                     {
                         "path":
                             "partition_0_65535\/deploy_meta.3",
                         "status":
                             "DONE"
                     }
                 ],
                "unfinishedTimestamp" : 0,
                "scheduleStatus" : 0,
                "priority" : 0,
                "srcRoot" : "srcRoot"
            }
        ],
        "errorInfo": {},
        "status": 4
    }
    )";
    return madroxInJson;
}
std::string SyncIndexTaskTest::getCancelMadroxInfo() const
{
    string madroxInJson = R"(
    {
        "dest" :
        [
            {
                "destRoot" : "hdfs://abx/",
                "finishedItemCount" : 1000,
                "failedItemCount" : 0,
                "totalItemCount" : 10,
                "finishTimestamp" : 2000,
                "itemDetail" : [],
                "canceledCount" : 0,
                "pendingRequests" : 0,
                "lastUpdateTimestamp" : 2000,
                "initTimestamp" : 1000,
                "locked" : false,
                "destHash" : 0,
                "configId" : 0,
                "deployMetaDescription" : "minVersion=1;maxVersion=3;partitionCount=32;metaCount=64",
                "deployMetaDetail":
                 [
                     {
                         "path":
                             "partition_0_65535\/deploy_meta.1",
                         "status":
                             "CANCELED"
                     },
                     {
                         "path":
                             "partition_0_65535\/deploy_meta.3",
                         "status":
                             "DONE"
                     }
                 ],
                "unfinishedTimestamp" : 0,
                "scheduleStatus" : 0,
                "priority" : 0,
                "srcRoot" : "srcRoot"
            }
        ],
        "errorInfo": {},
        "status": 4
    }
    )";
    return madroxInJson;
}

std::string SyncIndexTaskTest::getFailedMadroxInfo() const
{
    string madroxInJson = R"(
    {
        "dest" :
        [
            {
                "destRoot" : "hdfs://abx/",
                "finishedItemCount" : 1000,
                "failedItemCount" : 0,
                "totalItemCount" : 10,
                "finishTimestamp" : 2000,
                "itemDetail" : [],
                "canceledCount" : 0,
                "pendingRequests" : 0,
                "lastUpdateTimestamp" : 2000,
                "initTimestamp" : 1000,
                "locked" : false,
                "destHash" : 0,
                "configId" : 0,
                "deployMetaDescription" : "minVersion=1;maxVersion=3;partitionCount=32;metaCount=64",
                "deployMetaDetail":
                 [
                     {
                         "path":
                             "partition_0_65535\/deploy_meta.1",
                         "status":
                             "FAILED"
                     }
                 ],
                "unfinishedTimestamp" : 0,
                "scheduleStatus" : 0,
                "priority" : 0,
                "srcRoot" : "srcRoot"
            }
        ],
        "errorInfo": {},
        "status": 4
    }
    )";
    return madroxInJson;
}

TEST_F(SyncIndexTaskTest, testHandleTargetSimpleProcess)
{
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/sync_index_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "cluster1";

    initParam.buildId.appName = _pid.buildid().appname();
    initParam.buildId.dataTable = _pid.buildid().datatable();
    initParam.buildId.generationId = _pid.buildid().generationid();
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 1;

    ASSERT_TRUE(_syncIndexTask->init(initParam));
    _syncIndexTask->_madroxChannel = _mockMadroxChannel;

    config::TaskTarget taskTarget;
    vector<versionid_t> targetVersion {1, 3};
    taskTarget.addTargetDescription(config::MADROX_ADMIN_ADDRESS, "zfs://madrox/master");
    taskTarget.addTargetDescription("versions", ToJsonString(targetVersion));

    madrox::proto::UpdateResponse updateRet1;
    madrox::proto::UpdateResponse updateRet2;

    EXPECT_CALL(*_mockMadroxChannel, UpdateRequest(_, _)).WillOnce(DoAll(SetArgReferee<1>(updateRet1), Return(true)));

    madrox::proto::GetStatusResponse mgsRet1;
    madrox::proto::GetStatusResponse mgsRet2;

    auto madroxInJson1 = getCancelMadroxInfo();
    auto madroxInJson2 = getNormalMadroxInfo();
    auto status1 = google::protobuf::util::JsonStringToMessage(madroxInJson1, &mgsRet1);
    ASSERT_TRUE(status1.ok());
    auto status2 = google::protobuf::util::JsonStringToMessage(madroxInJson2, &mgsRet2);
    ASSERT_TRUE(status2.ok());

    EXPECT_CALL(*_mockMadroxChannel, GetStatus(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(mgsRet1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(mgsRet2), Return(true)));

    taskTarget.addTargetDescription(BS_SYNC_INDEX_ROOT, "hdfs://abx/");
    _syncIndexTask->_madroxRoot = "zfs://madrox/master";
    ASSERT_TRUE(_syncIndexTask->handleTarget(taskTarget));
    bool done = false;
    for (int i = 0; i < 10; ++i) {
        if (_syncIndexTask->isDone(taskTarget)) {
            done = true;
            break;
        }
        usleep(3 * 1000 * 3000);
    }
    ASSERT_TRUE(done);
}

TEST_F(SyncIndexTaskTest, testNoNeedSync)
{
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/sync_index_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "cluster1";

    initParam.buildId.appName = _pid.buildid().appname();
    initParam.buildId.dataTable = _pid.buildid().datatable();
    initParam.buildId.generationId = _pid.buildid().generationid();
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 1;

    ASSERT_TRUE(_syncIndexTask->init(initParam));
    _syncIndexTask->_madroxChannel = _mockMadroxChannel;

    config::TaskTarget taskTarget;
    vector<versionid_t> targetVersion {1, 3};
    taskTarget.addTargetDescription(config::MADROX_ADMIN_ADDRESS, "zfs://madrox/master");
    taskTarget.addTargetDescription("versions", ToJsonString(targetVersion));

    taskTarget.addTargetDescription(BS_SYNC_INDEX_ROOT, _indexRoot);
    _syncIndexTask->_madroxRoot = "zfs://madrox/master";
    ASSERT_TRUE(_syncIndexTask->handleTarget(taskTarget));
    bool done = false;
    for (int i = 0; i < 10; ++i) {
        if (_syncIndexTask->isDone(taskTarget)) {
            done = true;
            break;
        }
        usleep(3 * 1000 * 3000);
    }
    ASSERT_TRUE(done);
}

TEST_F(SyncIndexTaskTest, testSyncFailed)
{
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/sync_index_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "cluster1";

    initParam.buildId.appName = _pid.buildid().appname();
    initParam.buildId.dataTable = _pid.buildid().datatable();
    initParam.buildId.generationId = _pid.buildid().generationid();
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 1;

    ASSERT_TRUE(_syncIndexTask->init(initParam));
    _syncIndexTask->_madroxChannel = _mockMadroxChannel;

    config::TaskTarget taskTarget;
    vector<versionid_t> targetVersion {1};
    taskTarget.addTargetDescription(config::MADROX_ADMIN_ADDRESS, "zfs://madrox/master");
    taskTarget.addTargetDescription("versions", ToJsonString(targetVersion));

    madrox::proto::UpdateResponse updateRet1;
    madrox::proto::UpdateResponse updateRet2;

    EXPECT_CALL(*_mockMadroxChannel, UpdateRequest(_, _))
        .WillRepeatedly(DoAll(SetArgReferee<1>(updateRet1), Return(true)));

    EXPECT_CALL(*_mockMadroxChannel, GetStatus(_, _)).WillRepeatedly(Invoke(this, &SyncIndexTaskTest::doGetStatus));

    taskTarget.addTargetDescription(BS_SYNC_INDEX_ROOT, "hdfs://abx/");
    _syncIndexTask->_madroxRoot = "zfs://madrox/master";
    ASSERT_TRUE(_syncIndexTask->handleTarget(taskTarget));
    bool done = false;
    for (int i = 0; i < 7; ++i) {
        if (_syncIndexTask->isDone(taskTarget)) {
            done = true;
            break;
        }
        usleep(3 * 1000 * 1000);
    }
    ASSERT_FALSE(done);
    string currentStatus = _syncIndexTask->getTaskStatus();
    KeyValueMap kvMap;
    FromJsonString(kvMap, currentStatus);
    ASSERT_EQ(KV_TRUE, kvMap["need_update_target"]);

    taskTarget.updateTargetDescription("versions", ToJsonString(vector<versionid_t> {1, 3}));
    ASSERT_TRUE(_syncIndexTask->handleTarget(taskTarget));
    currentStatus = _syncIndexTask->getTaskStatus();
    kvMap.clear();
    FromJsonString(kvMap, currentStatus);
    ASSERT_NE(KV_TRUE, kvMap["need_update_target"]);
    for (int i = 0; i < 7; ++i) {
        if (_syncIndexTask->isDone(taskTarget)) {
            done = true;
            break;
        }
        usleep(3 * 1000 * 1000);
    }
    ASSERT_TRUE(done);
}

}} // namespace build_service::task_base
