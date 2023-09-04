#include "build_service/config/CLIOptionNames.h"
#include "build_service_tasks/channel/test/FakeRpcServer.h"
#include "build_service_tasks/channel/test/MockRpcChannel.h"
#include "fslib/util/FileUtil.h"
// Workaround for test private/protected member
#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "build_service_tasks/cloneIndex/CloneIndexTask.h"
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

class CloneIndexTaskTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    void WriteSpecFile(const std::string& ip, uint32_t port, const std::string& baseDir);

protected:
    proto::PartitionId _pid;
    std::shared_ptr<MockMadroxChannel> _mockMadroxChannel;
    std::shared_ptr<MockBsAdminChannel> _mockBsAdminChannel;
    std::unique_ptr<CloneIndexTask> _cloneIndexTask;
    proto::InformResponse _response;
    cm_basic::ZkWrapper _mock;
};

void CloneIndexTaskTest::setUp()
{
    _cloneIndexTask = std::make_unique<CloneIndexTask>();
    _mockBsAdminChannel.reset(new MockBsAdminChannel());
    _mockMadroxChannel.reset(new MockMadroxChannel());
    _pid = proto::PartitionId();
    _pid.set_role(proto::ROLE_TASK);
    _pid.set_step(proto::BUILD_STEP_INC);
    _pid.mutable_range()->set_from(0);
    _pid.mutable_range()->set_to(65535);
    _pid.add_clusternames("allCluster");
    _pid.mutable_buildid()->set_appname("testapp");
    _pid.mutable_buildid()->set_datatable("simple");
    _pid.mutable_buildid()->set_generationid(1234567);
    _cloneIndexTask->_madroxChannel = _mockMadroxChannel;
    _cloneIndexTask->_bsChannel = _mockBsAdminChannel;

    string jsonStr;
    fslib::util::FileUtil::readFile(GET_TEST_DATA_PATH() + "/clone_index_test/config/build_app.json.template", jsonStr);
    fslib::util::FileUtil::atomicCopy(GET_TEST_DATA_PATH() + "/clone_index_test/config",
                                      GET_TEMP_DATA_PATH() + "/clone_index_test/config");
    BuildServiceConfig BSConfig;
    FromJsonString(BSConfig, jsonStr);
    BSConfig._indexRoot = GET_TEMP_DATA_PATH();
    jsonStr = ToJsonString(BSConfig);
    fslib::util::FileUtil::writeFile(GET_TEMP_DATA_PATH() + "/clone_index_test/config/build_app.json", jsonStr);
}

void CloneIndexTaskTest::tearDown()
{
    _cloneIndexTask.reset();
    _mockBsAdminChannel.reset();
    _mockMadroxChannel.reset();
}
TEST_F(CloneIndexTaskTest, testGetParamFromTaskTarget)
{
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/clone_index_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "cluster1";

    initParam.buildId.appName = _pid.buildid().appname();
    initParam.buildId.dataTable = _pid.buildid().datatable();
    initParam.buildId.generationId = _pid.buildid().generationid();
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 1;
    ASSERT_TRUE(_cloneIndexTask->init(initParam));

    config::TaskTarget taskTarget;
    vector<pair<string, versionid_t>> targetClusters;

    // false, empty config::SOURCE_INDEX_ADMIN_ADDRESS
    ASSERT_FALSE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));

    // false, empty config::SOURCE_INDEX_BUILD_ID
    taskTarget.addTargetDescription(config::SOURCE_INDEX_ADMIN_ADDRESS, "");
    ASSERT_FALSE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));

    // false, empty config::MADROX_ADMIN_ADDRESS
    std::string invalidSrcBuildId = R"(invalid)";
    std::string srcBuildId = R"({"dataTable":"data", "generationId":123, "appName":"appName"})";
    taskTarget.addTargetDescription(config::SOURCE_INDEX_BUILD_ID, "");
    ASSERT_FALSE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));

    // false, empty config::SOURCE_INDEX_BUILD_ID
    taskTarget.addTargetDescription(config::MADROX_ADMIN_ADDRESS, "");
    ASSERT_FALSE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));

    // false, empty config::SOURCE_INDEX_ADMIN_ADDRESS
    taskTarget.addTargetDescription(config::SOURCE_INDEX_ADMIN_ADDRESS, "");
    ASSERT_FALSE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));

    std::string invalidFormatClusterNames = "invalid_clusterNames: {";
    std::string emptyClusterNames = "[]";
    std::string configNotExistClusterNames = R"(["cluster1", "cluster2", "cluster3"])";
    std::string clusterNames = R"(["cluster1", "cluster2"])";
    // false, invalid format
    taskTarget.addTargetDescription("clusterNames", invalidFormatClusterNames);
    ASSERT_FALSE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));
    // false, invalid build id
    taskTarget.updateTargetDescription("clusterNames", emptyClusterNames);
    ASSERT_FALSE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));

    // false, empty clusters
    taskTarget.updateTargetDescription(config::SOURCE_INDEX_BUILD_ID, srcBuildId);
    ASSERT_FALSE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));

    // // false, cluster not in config
    // taskTarget.updateTargetDescription("clusterNames", configNotExistClusterNames);
    // ASSERT_TRUE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));

    // true
    taskTarget.updateTargetDescription("clusterNames", clusterNames);
    ASSERT_TRUE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));
    vector<pair<string, versionid_t>> expectClusters({make_pair("cluster1", -1), make_pair("cluster2", -1)});
    ASSERT_EQ(expectClusters, targetClusters);

    // false add not equal versions
    taskTarget.updateTargetDescription("versionIds", "[1]");
    ASSERT_FALSE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));

    // true
    taskTarget.updateTargetDescription("versionIds", "[1,100]");
    targetClusters.clear();
    ASSERT_TRUE(_cloneIndexTask->getParamFromTaskTarget(taskTarget, targetClusters));
    expectClusters.clear();
    expectClusters.push_back(make_pair("cluster1", 1));
    expectClusters.push_back(make_pair("cluster2", 100));
    ASSERT_EQ(expectClusters, targetClusters);
}

TEST_F(CloneIndexTaskTest, testGetMadroxStatus)
{
    std::string strExistErrResp = R"({
        "dest" :[
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
                    "deployMetaDescription" : "descr",
                    "unfinishedTimestamp" : 0,
                    "scheduleStatus" : 0,
                    "priority" : 0,
                    "srcRoot" : "srcRoot"
                }

        ],
        "errorInfo" : {"errorCode" : 2},
        "status":4
    })";
    // "
    ::madrox::proto::GetStatusResponse existErrResp;
    ::madrox::proto::GetStatusResponse emptyDestResp;
    ::madrox::proto::GetStatusResponse runningResp;
    ::madrox::proto::GetStatusResponse cancelResp;
    ::madrox::proto::GetStatusResponse doneResp;
    ::madrox::proto::GetStatusResponse defaultResp;
    auto status = ::google::protobuf::util::JsonStringToMessage(strExistErrResp, &existErrResp);
    ASSERT_TRUE(status.ok());

    auto ec = ::madrox::proto::MasterErrorCode::MASTER_ERROR_NONE;
    emptyDestResp.mutable_errorinfo()->set_errorcode(ec);
    runningResp = emptyDestResp;
    ::madrox::proto::DestCurrent dest;
    // ::madrox::proto::SyncStatus status;
    runningResp.add_dest()->CopyFrom(dest);
    runningResp.set_status(::madrox::proto::SyncStatus::RUNNING);
    cancelResp = runningResp;
    cancelResp.set_status(::madrox::proto::SyncStatus::CANCELED);
    doneResp = runningResp;
    doneResp.set_status(::madrox::proto::SyncStatus::DONE);
    defaultResp = runningResp;
    defaultResp.set_status(::madrox::proto::SyncStatus::LOCKED);

    EXPECT_CALL(*_mockMadroxChannel, GetStatus(::testing::_, ::testing::_))
        .WillOnce(Return(false))
        .WillOnce(DoAll(SetArgReferee<1>(existErrResp), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(emptyDestResp), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(runningResp), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(cancelResp), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(doneResp), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(defaultResp), Return(true)));

    std::string clusterName;
    std::string targetGenerationPath;
    // update rpc failed
    ASSERT_FALSE(_cloneIndexTask->getMadroxStatus(clusterName, targetGenerationPath));

    // update success, but have error message
    ASSERT_FALSE(_cloneIndexTask->getMadroxStatus(clusterName, targetGenerationPath));

    // update success, but empty dest
    ASSERT_FALSE(_cloneIndexTask->getMadroxStatus(clusterName, targetGenerationPath));

    // false, running
    ASSERT_FALSE(_cloneIndexTask->getMadroxStatus(clusterName, targetGenerationPath));
    // false, cancel
    ASSERT_FALSE(_cloneIndexTask->getMadroxStatus(clusterName, targetGenerationPath));
    // true, done
    ASSERT_TRUE(_cloneIndexTask->getMadroxStatus(clusterName, targetGenerationPath));

    // false, default
    ASSERT_FALSE(_cloneIndexTask->getMadroxStatus(clusterName, targetGenerationPath));
}

TEST_F(CloneIndexTaskTest, testCloneIndexRpcBlock)
{
    std::string clusterName1("cluster1");
    std::string clusterName2("cluster2");
    // false, not fount in _targetClusterMap
    ASSERT_FALSE(_cloneIndexTask->cloneIndexRpcBlock(clusterName1));
    CloneIndexTask::ClusterDescription clusterDesc1;
    clusterDesc1.isDone = true;
    CloneIndexTask::ClusterDescription clusterDesc2;
    clusterDesc2.isDone = true;
    _cloneIndexTask->_targetClusterMap[clusterName1] = clusterDesc1;
    // false, isDone
    ASSERT_TRUE(_cloneIndexTask->cloneIndexRpcBlock(clusterName1));
    _cloneIndexTask->_targetClusterMap[clusterName2] = clusterDesc2;
    ASSERT_TRUE(_cloneIndexTask->cloneIndexRpcBlock(clusterName1));
    (_cloneIndexTask->_targetClusterMap[clusterName1]).isDone = false;
    (_cloneIndexTask->_targetClusterMap[clusterName2]).isDone = false;

    std::string strExistErrResp = R"({"errorInfo" : { "errorCode" : 2}})";

    ::madrox::proto::UpdateResponse existErrResp;
    ::madrox::proto::UpdateResponse emptyInfoResp;
    auto status = ::google::protobuf::util::JsonStringToMessage(strExistErrResp, &existErrResp);
    ASSERT_TRUE(status.ok());

    EXPECT_CALL(*_mockMadroxChannel, UpdateRequest(::testing::_, ::testing::_))
        .WillOnce(Return(false))
        .WillOnce(DoAll(SetArgReferee<1>(existErrResp), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(emptyInfoResp), Return(true)));

    std::string clusterName;
    // update rpc failed
    ASSERT_FALSE(_cloneIndexTask->cloneIndexRpcBlock(clusterName1));

    // update success, but have error message
    ASSERT_FALSE(_cloneIndexTask->cloneIndexRpcBlock(clusterName1));

    // success
    ASSERT_TRUE(_cloneIndexTask->cloneIndexRpcBlock(clusterName1));
}

TEST_F(CloneIndexTaskTest, testGetServiceInfoRpcBlock)
{
    std::string strExistErrResp = R"({"errorMessage" : ["has_err"]})";
    proto::ServiceInfoResponse existErrResp;
    auto status = ::google::protobuf::util::JsonStringToMessage(strExistErrResp, &existErrResp);
    ASSERT_TRUE(status.ok());

    proto::ServiceInfoResponse emptyInfoResp;
    proto::ServiceInfoResponse notEmptyInfoResp;
    proto::GenerationInfo generationInfo;
    notEmptyInfoResp.add_generationinfos()->CopyFrom(generationInfo);
    EXPECT_CALL(*_mockBsAdminChannel, GetServiceInfo(::testing::_, ::testing::_))
        .WillOnce(Return(false))
        .WillOnce(DoAll(SetArgReferee<1>(existErrResp), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(emptyInfoResp), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(notEmptyInfoResp), Return(true)));

    proto::GenerationInfo resp;
    // gs rpc failed
    ASSERT_FALSE(_cloneIndexTask->getServiceInfoRpcBlock(resp));

    // gs success, but have error message
    ASSERT_FALSE(_cloneIndexTask->getServiceInfoRpcBlock(resp));

    // gs success, but empty gs info
    ASSERT_FALSE(_cloneIndexTask->getServiceInfoRpcBlock(resp));

    // success
    ASSERT_TRUE(_cloneIndexTask->getServiceInfoRpcBlock(resp));
    std::string strInGSInfo;
    std::string strOutGSInfo;
    status = ::google::protobuf::util::MessageToJsonString(generationInfo, &strInGSInfo);
    ASSERT_TRUE(status.ok());
    status = ::google::protobuf::util::MessageToJsonString(resp, &strOutGSInfo);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(strInGSInfo, strOutGSInfo);
}

TEST_F(CloneIndexTaskTest, testCreateSnapshot)
{
    std::string strExistErrResp = R"({"errorMessage" : ["has_err"]})";
    proto::InformResponse existErrResp;
    auto status = ::google::protobuf::util::JsonStringToMessage(strExistErrResp, &existErrResp);
    ASSERT_TRUE(status.ok());

    std::string srcIndexBuildId = "";
    EXPECT_CALL(*_mockBsAdminChannel, CreateSnapshot(::testing::_, ::testing::_))
        .WillOnce(Return(false))
        .WillOnce(DoAll(SetArgReferee<1>(existErrResp), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(_response), Return(true)));
    // invalid buildId
    ASSERT_FALSE(_cloneIndexTask->createSnapshot("cluster1", 100));
    srcIndexBuildId = R"({"dataTable" : "dataTable", "generationId" : 100, "NotExistField" : "appName"})";
    _cloneIndexTask->_srcIndexBuildId = srcIndexBuildId;
    ASSERT_FALSE(_cloneIndexTask->createSnapshot("cluster1", 100));

    // false, rpc failed
    srcIndexBuildId = R"({"dataTable" : "dataTable", "generationId" : 100, "appName" : "appName"})";
    _cloneIndexTask->_srcIndexBuildId = srcIndexBuildId;
    ASSERT_FALSE(_cloneIndexTask->createSnapshot("cluster1", 100));

    // createSnap success, but have error
    ASSERT_FALSE(_cloneIndexTask->createSnapshot("cluster1", 100));

    // success
    ASSERT_TRUE(_cloneIndexTask->createSnapshot("cluster1", 100));
}

TEST_F(CloneIndexTaskTest, testRemoveSnapshot)
{
    std::string strExistErrResp = R"({"errorMessage" : ["has_err"]})";
    proto::InformResponse existErrResp;
    auto status = ::google::protobuf::util::JsonStringToMessage(strExistErrResp, &existErrResp);
    ASSERT_TRUE(status.ok());

    EXPECT_CALL(*_mockBsAdminChannel, RemoveSnapshot(::testing::_, ::testing::_))
        .WillOnce(Return(false))
        .WillOnce(DoAll(SetArgReferee<1>(existErrResp), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(_response), Return(true)));

    (_cloneIndexTask->_targetClusterMap)["cluster1"].sourceVersionId = 100;

    // remove rpc failed
    ASSERT_FALSE(_cloneIndexTask->removeSnapshots("cluster1"));

    // removeSnapshot success, but have error message
    ASSERT_FALSE(_cloneIndexTask->removeSnapshots("cluster1"));

    // success
    ASSERT_TRUE(_cloneIndexTask->removeSnapshots("cluster1"));
}

TEST_F(CloneIndexTaskTest, testInit)
{
    config::TaskTarget taskTarget;
    Task::TaskInitParam taskInitParam;
    ASSERT_FALSE(_cloneIndexTask->init(taskInitParam));
    ASSERT_FALSE(_cloneIndexTask->isDone(taskTarget));

    std::string clusterName = "cluster1";
    taskInitParam.clusterName = clusterName;
    ASSERT_TRUE(_cloneIndexTask->init(taskInitParam));
    ASSERT_FALSE(_cloneIndexTask->isDone(taskTarget));
}

namespace {
std::string getNormalGenerationInfo()
{
    string gsInJson = R"(
        {
            "generationInfos" : [
                {
                    "indexRoot" : "pangu://testIndexRoot/",
                    "buildInfo" : {
                        "clusterInfos" : [
                            {
                                "clusterName" : "cluster1",
                                "partitionCount" : 2,

                            },
                            {
                                "clusterName" : "cluster2",
                                "partitionCount" : 2,
                            }
                        ]
                    },
                    "indexInfos" : [
                        {
                            "clusterName" : "cluster1",
                            "range" : {"from" : 0, "to" : 32767},
                            "versionTimestamp" : 1000001,
                            "processorCheckpoint" : 1000001,
                            "indexVersion" : 3
                        },
                        {
                            "clusterName" : "cluster1",
                            "range" : {"from" : 32768, "to" : 65535},
                            "versionTimestamp" : 2000001,
                            "processorCheckpoint" : 1000001,
                            "indexVersion" : 3
                        },
                        {
                            "clusterName" : "cluster2",
                            "range" : {"from" : 0, "to" : 32767},
                            "versionTimestamp" : 2000003,
                            "processorCheckpoint" : 1000003,
                            "indexVersion" : 2
                        },
                        {
                            "clusterName" : "cluster2",
                            "range" : {"from" : 32768, "to" : 65535},
                            "versionTimestamp" : 2000003,
                            "processorCheckpoint" : 1000001,
                            "indexVersion" : 2
                        }
                    ]
                }
            ],
            "errorMessage" : []

        }
    )";
    return gsInJson;
}

std::string getBuildInfoGenerationInfo()
{
    string gsInJson = R"(
        {
            "generationInfos" : [
                {
                    "indexRoot" : "pangu://testIndexRoot/",
                    "indexInfos" : [
                        {
                            "clusterName" : "cluster1",
                            "range" : {"from" : 0, "to" : 32767},
                            "versionTimestamp" : 1000001,
                            "processorCheckpoint" : 1000001,
                            "indexVersion" : 3
                        },
                        {
                            "clusterName" : "cluster1",
                            "range" : {"from" : 32768, "to" : 65535},
                            "versionTimestamp" : 2000001,
                            "processorCheckpoint" : 1000001,
                            "indexVersion" : 3
                        },
                        {
                            "clusterName" : "cluster2",
                            "range" : {"from" : 0, "to" : 32767},
                            "versionTimestamp" : 2000003,
                            "processorCheckpoint" : 1000003,
                            "indexVersion" : 2
                        },
                        {
                            "clusterName" : "cluster2",
                            "range" : {"from" : 32768, "to" : 65535},
                            "versionTimestamp" : 2000003,
                            "processorCheckpoint" : 1000001,
                            "indexVersion" : 2
                        }
                    ],
                    "buildInfo" : {
                       "clusterInfos" : [
                         {
                           "checkpointInfos" : [{
                               "versionId" :  3,
                                "versionTimestamp" : 100000,
                                "processorCheckpoint" : 100001
                            }],
                            "partitionCount" : 2,
                            "clusterName" : "cluster1"
                            },
                            {
                           "checkpointInfos" : [{
                                "versionId" : 2,
                                "versionTimestamp" : 200000,
                                "processorCheckpoint" : 200001
                                }],
                               "partitionCount" : 2,
                               "clusterName" : "cluster2"
                           }
                       ]
                     }
                }
            ],
            "errorMessage" : []
        }
    )";

    return gsInJson;
}

std::string getNormalMadroxInfo()
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
                "deployMetaDescription" : "descr",
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
} // namespace

TEST_F(CloneIndexTaskTest, testHandleTargetSimpleProcess)
{
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/clone_index_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "cluster1";

    initParam.buildId.appName = _pid.buildid().appname();
    initParam.buildId.dataTable = _pid.buildid().datatable();
    initParam.buildId.generationId = _pid.buildid().generationid();
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 1;

    ASSERT_TRUE(_cloneIndexTask->init(initParam));
    _cloneIndexTask->_madroxChannel = _mockMadroxChannel;
    _cloneIndexTask->_bsChannel = _mockBsAdminChannel;

    config::TaskTarget taskTarget;
    taskTarget.addTargetDescription(config::SOURCE_INDEX_ADMIN_ADDRESS, "zfs://bsAdmin/admin");
    taskTarget.addTargetDescription(
        config::SOURCE_INDEX_BUILD_ID,
        "{\"dataTable\": \"alimama_20s\", \"generationId\": 1595400321, \"appName\": \"tisplus_khronos\"}");
    taskTarget.addTargetDescription(config::MADROX_ADMIN_ADDRESS, "zfs://madrox/master");
    taskTarget.addTargetDescription("clusterNames", "[\"cluster1\", \"cluster2\"]");

    auto gsInJson = getNormalGenerationInfo();
    proto::ServiceInfoResponse gsResponse;
    auto status = google::protobuf::util::JsonStringToMessage(gsInJson, &gsResponse);
    ASSERT_TRUE(status.ok());

    EXPECT_CALL(*_mockBsAdminChannel, GetServiceInfo(_, _)).WillOnce(DoAll(SetArgReferee<1>(gsResponse), Return(true)));

    proto::InformResponse ret1;
    proto::InformResponse ret2;
    EXPECT_CALL(*_mockBsAdminChannel, CreateSnapshot(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(ret1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(ret2), Return(true)));

    madrox::proto::UpdateResponse updateRet1;
    madrox::proto::UpdateResponse updateRet2;

    EXPECT_CALL(*_mockMadroxChannel, UpdateRequest(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(updateRet1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(updateRet2), Return(true)));

    madrox::proto::GetStatusResponse mgsRet1;
    madrox::proto::GetStatusResponse mgsRet2;

    auto madroxInJson = getNormalMadroxInfo();
    status = google::protobuf::util::JsonStringToMessage(madroxInJson, &mgsRet1);
    ASSERT_TRUE(status.ok());
    mgsRet2 = mgsRet1;

    EXPECT_CALL(*_mockMadroxChannel, GetStatus(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(mgsRet1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(mgsRet2), Return(true)));

    proto::InformResponse rmsRet1;
    proto::InformResponse rmsRet2;

    EXPECT_CALL(*_mockBsAdminChannel, RemoveSnapshot(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(rmsRet1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(rmsRet2), Return(true)));

    ASSERT_TRUE(_cloneIndexTask->handleTarget(taskTarget));

    CloneIndexTask::ClusterMap clusterMap;
    bool recovered = false;
    ASSERT_TRUE(_cloneIndexTask->tryRecoverFromCheckpointFiles({make_pair("cluster1", -1), make_pair("cluster2", -1)},
                                                               clusterMap, recovered));
    ASSERT_TRUE(recovered);
    ASSERT_EQ(3, clusterMap["cluster1"].sourceVersionId);
    ASSERT_EQ(2, clusterMap["cluster2"].sourceVersionId);
    ASSERT_EQ(2, clusterMap["cluster1"].partitionCount);
    ASSERT_EQ(2, clusterMap["cluster2"].partitionCount);
    ASSERT_EQ(1000001, clusterMap["cluster1"].indexLocator);
    ASSERT_EQ(2000003, clusterMap["cluster2"].indexLocator);

    ASSERT_EQ(CloneIndexTask::getGenerationPath("pangu://testIndexRoot/", "cluster1", 1595400321),
              clusterMap["cluster1"].sourceIndexPath);
    ASSERT_EQ(CloneIndexTask::getGenerationPath("pangu://testIndexRoot/", "cluster2", 1595400321),
              clusterMap["cluster2"].sourceIndexPath);

    ASSERT_EQ(CloneIndexTask::getGenerationPath(GET_TEMP_DATA_PATH(), "cluster1", 1234567),
              clusterMap["cluster1"].targetIndexPath);
    ASSERT_EQ(CloneIndexTask::getGenerationPath(GET_TEMP_DATA_PATH(), "cluster2", 1234567),
              clusterMap["cluster2"].targetIndexPath);

    // ASSERT_EQ(2, clusterMap["cluster2"].sourceIndexPath);
    // ASSERT_EQ(2, clusterMap["cluster1"].targetIndexPath);
    // ASSERT_EQ(2, clusterMap["cluster2"].targetIndexPath);

    ASSERT_TRUE(_cloneIndexTask->handleTarget(taskTarget));

    KeyValueMap cluster2Locator;
    cluster2Locator["cluster1"] = "1000001";
    cluster2Locator["cluster2"] = "2000003";

    string expectedLocators = ToJsonString(cluster2Locator);
    ASSERT_EQ(expectedLocators, _cloneIndexTask->_kvMap[INDEX_CLONE_LOCATOR]);
}

TEST_F(CloneIndexTaskTest, testHandleTargetWithVersion)
{
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/clone_index_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "cluster1";

    initParam.buildId.appName = _pid.buildid().appname();
    initParam.buildId.dataTable = _pid.buildid().datatable();
    initParam.buildId.generationId = _pid.buildid().generationid();
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 1;

    ASSERT_TRUE(_cloneIndexTask->init(initParam));
    _cloneIndexTask->_madroxChannel = _mockMadroxChannel;
    _cloneIndexTask->_bsChannel = _mockBsAdminChannel;

    config::TaskTarget taskTarget;
    taskTarget.addTargetDescription(config::SOURCE_INDEX_ADMIN_ADDRESS, "zfs://bsAdmin/admin");
    taskTarget.addTargetDescription(
        config::SOURCE_INDEX_BUILD_ID,
        "{\"dataTable\": \"alimama_20s\", \"generationId\": 1595400321, \"appName\": \"tisplus_khronos\"}");
    taskTarget.addTargetDescription(config::MADROX_ADMIN_ADDRESS, "zfs://madrox/master");
    taskTarget.addTargetDescription("clusterNames", "[\"cluster1\", \"cluster2\"]");
    taskTarget.addTargetDescription("versionIds", "[3, 2]");

    auto gsInJson = getBuildInfoGenerationInfo();
    proto::ServiceInfoResponse gsResponse;
    auto status = google::protobuf::util::JsonStringToMessage(gsInJson, &gsResponse);
    ASSERT_TRUE(status.ok());

    EXPECT_CALL(*_mockBsAdminChannel, GetServiceInfo(_, _)).WillOnce(DoAll(SetArgReferee<1>(gsResponse), Return(true)));

    proto::InformResponse ret1;
    proto::InformResponse ret2;
    EXPECT_CALL(*_mockBsAdminChannel, CreateSnapshot(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(ret1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(ret2), Return(true)));

    madrox::proto::UpdateResponse updateRet1;
    madrox::proto::UpdateResponse updateRet2;

    EXPECT_CALL(*_mockMadroxChannel, UpdateRequest(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(updateRet1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(updateRet2), Return(true)));

    madrox::proto::GetStatusResponse mgsRet1;
    madrox::proto::GetStatusResponse mgsRet2;

    auto madroxInJson = getNormalMadroxInfo();
    status = google::protobuf::util::JsonStringToMessage(madroxInJson, &mgsRet1);
    ASSERT_TRUE(status.ok());
    mgsRet2 = mgsRet1;

    EXPECT_CALL(*_mockMadroxChannel, GetStatus(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(mgsRet1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(mgsRet2), Return(true)));

    proto::InformResponse rmsRet1;
    proto::InformResponse rmsRet2;

    EXPECT_CALL(*_mockBsAdminChannel, RemoveSnapshot(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(rmsRet1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(rmsRet2), Return(true)));

    ASSERT_TRUE(_cloneIndexTask->handleTarget(taskTarget));
    ASSERT_EQ(2, _cloneIndexTask->_targetClusterMap.size());
    ASSERT_EQ(3, _cloneIndexTask->_targetClusterMap["cluster1"].sourceVersionId);
    ASSERT_EQ(2, _cloneIndexTask->_targetClusterMap["cluster2"].sourceVersionId);
    ASSERT_EQ(2, _cloneIndexTask->_targetClusterMap["cluster1"].partitionCount);
    ASSERT_EQ(2, _cloneIndexTask->_targetClusterMap["cluster2"].partitionCount);
    ASSERT_EQ(100000, _cloneIndexTask->_targetClusterMap["cluster1"].indexLocator);
    ASSERT_EQ(200000, _cloneIndexTask->_targetClusterMap["cluster2"].indexLocator);

    CloneIndexTask::ClusterMap clusterMap;
    bool recovered = false;
    ASSERT_TRUE(_cloneIndexTask->tryRecoverFromCheckpointFiles({make_pair("cluster1", 3), make_pair("cluster2", 2)},
                                                               clusterMap, recovered));
    ASSERT_TRUE(recovered);
    ASSERT_EQ(3, clusterMap["cluster1"].sourceVersionId);
    ASSERT_EQ(2, clusterMap["cluster2"].sourceVersionId);
    ASSERT_EQ(2, clusterMap["cluster1"].partitionCount);
    ASSERT_EQ(2, clusterMap["cluster2"].partitionCount);
    ASSERT_EQ(100000, clusterMap["cluster1"].indexLocator);
    ASSERT_EQ(200000, clusterMap["cluster2"].indexLocator);

    ASSERT_EQ(CloneIndexTask::getGenerationPath("pangu://testIndexRoot/", "cluster1", 1595400321),
              clusterMap["cluster1"].sourceIndexPath);
    ASSERT_EQ(CloneIndexTask::getGenerationPath("pangu://testIndexRoot/", "cluster2", 1595400321),
              clusterMap["cluster2"].sourceIndexPath);

    ASSERT_EQ(CloneIndexTask::getGenerationPath(GET_TEMP_DATA_PATH(), "cluster1", 1234567),
              clusterMap["cluster1"].targetIndexPath);
    ASSERT_EQ(CloneIndexTask::getGenerationPath(GET_TEMP_DATA_PATH(), "cluster2", 1234567),
              clusterMap["cluster2"].targetIndexPath);

    // ASSERT_EQ(2, clusterMap["cluster2"].sourceIndexPath);
    // ASSERT_EQ(2, clusterMap["cluster1"].targetIndexPath);
    // ASSERT_EQ(2, clusterMap["cluster2"].targetIndexPath);

    ASSERT_TRUE(_cloneIndexTask->handleTarget(taskTarget));

    KeyValueMap cluster2Locator;
    cluster2Locator["cluster1"] = "100000";
    cluster2Locator["cluster2"] = "200000";

    string expectedLocators = ToJsonString(cluster2Locator);
    ASSERT_EQ(expectedLocators, _cloneIndexTask->_kvMap[INDEX_CLONE_LOCATOR]);
}

TEST_F(CloneIndexTaskTest, testGetLastVersionId)
{
    std::string validClusterName("cluster1");
    std::string InvalidClusterName("cluster_not_exist");
    uint32_t versionId = 0;
    uint32_t realVersionId = 0;

    proto::GenerationInfo generationInfo;
    // false empty indexinfos
    ASSERT_FALSE(_cloneIndexTask->getLastVersionId(generationInfo, validClusterName, realVersionId));
    ASSERT_EQ(0, versionId);

    // cluster not found
    proto::IndexInfo indexInfo1;
    proto::IndexInfo indexInfo2;
    std::string clusterName1 = ("cluster2");
    std::string clusterName2 = validClusterName;
    uint32_t versionId1 = 100;
    uint32_t versionId2 = 200;
    indexInfo1.set_clustername(clusterName1);
    indexInfo1.set_indexversion(versionId1);
    indexInfo2.set_clustername(clusterName2);
    indexInfo2.set_indexversion(versionId2);
    generationInfo.add_indexinfos()->CopyFrom(indexInfo1);
    ASSERT_FALSE(_cloneIndexTask->getLastVersionId(generationInfo, validClusterName, realVersionId));
    ASSERT_EQ(0, versionId);

    // success
    generationInfo.add_indexinfos()->CopyFrom(indexInfo2);
    ASSERT_TRUE(_cloneIndexTask->getLastVersionId(generationInfo, validClusterName, realVersionId));
    ASSERT_EQ(200, realVersionId);
}

TEST_F(CloneIndexTaskTest, testCheckAndGetClusterInfos)
{
    proto::GenerationInfo generationInfo;
    // false, not exist buildinfo
    ASSERT_FALSE(_cloneIndexTask->checkAndGetClusterInfos(generationInfo));

    // false, empty buildinfo
    proto::BuildInfo buildInfo;
    generationInfo.mutable_buildinfo()->CopyFrom(buildInfo);
    ASSERT_FALSE(_cloneIndexTask->checkAndGetClusterInfos(generationInfo));

    // TODO(xiuchen), if clusterinfos not exist, call clusterinfos_size() return what?
    proto::SingleClusterInfo singleClusterInfo;
    std::string clusterName1("cluster1");
    std::string clusterName2("cluster2");
    buildInfo.add_clusterinfos()->CopyFrom(singleClusterInfo);
    generationInfo.mutable_buildinfo()->CopyFrom(buildInfo);

    // false, not exist clusterName
    ASSERT_FALSE(_cloneIndexTask->checkAndGetClusterInfos(generationInfo));
    // false, not exist partitionCount
    singleClusterInfo.set_clustername(clusterName1);
    buildInfo.clear_clusterinfos();
    buildInfo.add_clusterinfos()->CopyFrom(singleClusterInfo);
    generationInfo.mutable_buildinfo()->CopyFrom(buildInfo);
    ASSERT_FALSE(_cloneIndexTask->checkAndGetClusterInfos(generationInfo));

    // succes, empty _targetClusterMap
    uint32_t partCnt = 2;
    singleClusterInfo.set_partitioncount(partCnt);
    buildInfo.clear_clusterinfos();
    buildInfo.add_clusterinfos()->CopyFrom(singleClusterInfo);
    generationInfo.mutable_buildinfo()->CopyFrom(buildInfo);
    ASSERT_TRUE(_cloneIndexTask->checkAndGetClusterInfos(generationInfo));

    // false, mismatch partCount
    CloneIndexTask::ClusterDescription clusterDesc1;
    clusterDesc1.partitionCount = partCnt + 1;
    CloneIndexTask::ClusterDescription clusterDesc2;
    clusterDesc2.partitionCount = partCnt + 1;

    _cloneIndexTask->_targetClusterMap.insert(make_pair(clusterName1, clusterDesc1));
    ASSERT_FALSE(_cloneIndexTask->checkAndGetClusterInfos(generationInfo));
    (_cloneIndexTask->_targetClusterMap[clusterName1]).partitionCount = partCnt;
    _cloneIndexTask->_targetClusterMap.insert(make_pair(clusterName2, clusterDesc2));
    ASSERT_FALSE(_cloneIndexTask->checkAndGetClusterInfos(generationInfo));
    (_cloneIndexTask->_targetClusterMap[clusterName2]).partitionCount = partCnt;

    // false, mismatch target cluster count
    ASSERT_FALSE(_cloneIndexTask->checkAndGetClusterInfos(generationInfo));

    proto::SingleClusterInfo singleClusterInfo2;
    singleClusterInfo2.set_clustername(clusterName2);
    singleClusterInfo2.set_partitioncount(partCnt);
    buildInfo.add_clusterinfos()->CopyFrom(singleClusterInfo2);
    generationInfo.mutable_buildinfo()->CopyFrom(buildInfo);
    // success
    ASSERT_TRUE(_cloneIndexTask->checkAndGetClusterInfos(generationInfo));
}

TEST_F(CloneIndexTaskTest, testHandleTargetAbnormal)
{
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/clone_index_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "cluster1";

    initParam.buildId.appName = _pid.buildid().appname();
    initParam.buildId.dataTable = _pid.buildid().datatable();
    initParam.buildId.generationId = _pid.buildid().generationid();
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 1;

    ASSERT_TRUE(_cloneIndexTask->init(initParam));
    _cloneIndexTask->_madroxChannel = _mockMadroxChannel;
    _cloneIndexTask->_bsChannel = _mockBsAdminChannel;

    config::TaskTarget taskTarget;
    taskTarget.addTargetDescription(config::SOURCE_INDEX_ADMIN_ADDRESS, "zfs://bsAdmin/admin");
    taskTarget.addTargetDescription(
        config::SOURCE_INDEX_BUILD_ID,
        "{\"dataTable\": \"alimama_20s\", \"generationId\": 1595400321, \"appName\": \"tisplus_khronos\"}");
    taskTarget.addTargetDescription(config::MADROX_ADMIN_ADDRESS, "zfs://madrox/master");
    taskTarget.addTargetDescription("clusterNames", "[\"cluster1\", \"cluster2\"]");

    proto::ServiceInfoResponse gsResponse;
    auto gsInJson = getNormalGenerationInfo();

    auto status = google::protobuf::util::JsonStringToMessage(gsInJson, &gsResponse);
    ASSERT_TRUE(status.ok());

    // 1. rpc gs failed
    // 2. rpc create snapshot failed
    // 3. rpc create snapshot success and rpc clone index failed
    // 4. rpc clone index success
    EXPECT_CALL(*_mockBsAdminChannel, GetServiceInfo(_, _))
        .WillOnce((Return(false)))
        .WillOnce(DoAll(SetArgReferee<1>(gsResponse), Return(true)));

    // false，rpc gs info failed
    ASSERT_FALSE(_cloneIndexTask->handleTarget(taskTarget));
    proto::InformResponse ret1;
    proto::InformResponse ret2;
    EXPECT_CALL(*_mockBsAdminChannel, CreateSnapshot(_, _))
        .WillOnce((Return(false)))
        .WillOnce(DoAll(SetArgReferee<1>(ret1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(ret1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(ret2), Return(true)));

    madrox::proto::UpdateResponse updateRet1;
    madrox::proto::UpdateResponse updateRet2;

    EXPECT_CALL(*_mockMadroxChannel, UpdateRequest(_, _))
        .WillOnce((Return(false)))
        .WillOnce(DoAll(SetArgReferee<1>(updateRet1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(updateRet2), Return(true)));

    madrox::proto::GetStatusResponse mgsRet1;
    madrox::proto::GetStatusResponse mgsRet2;

    auto madroxInJson = getNormalMadroxInfo();

    status = google::protobuf::util::JsonStringToMessage(madroxInJson, &mgsRet1);
    ASSERT_TRUE(status.ok());
    mgsRet2 = mgsRet1;

    EXPECT_CALL(*_mockMadroxChannel, GetStatus(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(mgsRet1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(mgsRet2), Return(true)));

    proto::InformResponse rmsRet1;
    proto::InformResponse rmsRet2;

    EXPECT_CALL(*_mockBsAdminChannel, RemoveSnapshot(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(rmsRet1), Return(true)))
        .WillOnce(DoAll(SetArgReferee<1>(rmsRet2), Return(true)));

    ASSERT_FALSE(_cloneIndexTask->handleTarget(taskTarget));
    ASSERT_FALSE(_cloneIndexTask->handleTarget(taskTarget));
    ASSERT_TRUE(_cloneIndexTask->handleTarget(taskTarget));
    ASSERT_TRUE(_cloneIndexTask->handleTarget(taskTarget));

    KeyValueMap cluster2Locator;
    cluster2Locator["cluster1"] = "1000001";
    cluster2Locator["cluster2"] = "2000003";

    string expectedLocators = ToJsonString(cluster2Locator);
    ASSERT_EQ(expectedLocators, _cloneIndexTask->_kvMap[INDEX_CLONE_LOCATOR]);
}

void CloneIndexTaskTest::WriteSpecFile(const std::string& ip, uint32_t port, const std::string& baseDir)
{
    worker_framework::LeaderInfo leaderInfo;
    leaderInfo.setHttpAddress(ip, 80);
    string address = ip + ":" + StringUtil::toString(port);
    leaderInfo.set("address", address);
    leaderInfo.set("port", port);
    string specContent = leaderInfo.toString();

    ASSERT_TRUE(fslib::util::FileUtil::mkDir(baseDir + "/LeaderElection", true));
    ASSERT_TRUE(fslib::util::FileUtil::writeFile(baseDir + "/LeaderElection/leader_election001", specContent));
}

TEST_F(CloneIndexTaskTest, testFakeRpcServer)
{
    auto fakeBs = make_unique<FakeBsAdmin>();
    ASSERT_TRUE(fakeBs->init(0, nullptr));
    ASSERT_TRUE(fakeBs->startServer());
    cout << fakeBs->getIp() << ":" << fakeBs->getPort() << endl;

    auto fakeMadrox = make_unique<FakeMadroxService>();
    ASSERT_TRUE(fakeMadrox->init(0, nullptr));
    ASSERT_TRUE(fakeMadrox->startServer());
    cout << fakeMadrox->getIp() << ":" << fakeMadrox->getPort() << endl;

    string bsSpecPath = fslib::util::FileUtil::joinFilePath(GET_TEMP_DATA_PATH(), "bs");
    string madroxSpecPath = fslib::util::FileUtil::joinFilePath(GET_TEMP_DATA_PATH(), "madrox");

    WriteSpecFile(fakeBs->getIp(), fakeBs->getPort(), GET_TEMP_DATA_PATH() + "bs/admin/");
    WriteSpecFile(fakeMadrox->getIp(), fakeMadrox->getPort(), GET_TEMP_DATA_PATH() + "madrox/master/");

    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/clone_index_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "allCluster";
    initParam.buildId.appName = _pid.buildid().appname();
    initParam.buildId.dataTable = _pid.buildid().datatable();
    initParam.buildId.generationId = _pid.buildid().generationid();
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 1;

    auto cloneIndexTask = std::make_unique<CloneIndexTask>();
    ASSERT_TRUE(cloneIndexTask->init(initParam));

    config::TaskTarget taskTarget;
    taskTarget.addTargetDescription(config::SOURCE_INDEX_ADMIN_ADDRESS, bsSpecPath);
    taskTarget.addTargetDescription(
        config::SOURCE_INDEX_BUILD_ID,
        "{\"dataTable\": \"alimama_20s\", \"generationId\": 1595400321, \"appName\": \"tisplus_khronos\"}");
    taskTarget.addTargetDescription(config::MADROX_ADMIN_ADDRESS, madroxSpecPath);
    taskTarget.addTargetDescription("clusterNames", "[\"cluster1\", \"cluster2\"]");

    // bs return empty generationInfo
    ASSERT_FALSE(cloneIndexTask->handleTarget(taskTarget));
}

TEST_F(CloneIndexTaskTest, testBsAdminChannel)
{
    auto fakeBs = make_unique<FakeBsAdmin>();
    ASSERT_TRUE(fakeBs->init(0, nullptr));
    ASSERT_TRUE(fakeBs->startServer());

    string bsSpecRoot = fslib::util::FileUtil::joinFilePath(GET_TEMP_DATA_PATH(), "bs");
    auto bsChannel = make_shared<BsAdminChannel>("zfs:///" + GET_TEMP_DATA_PATH() + "bs");
    ASSERT_TRUE(bsChannel->init());
    WriteSpecFile(fakeBs->getIp(), fakeBs->getPort(), GET_TEMP_DATA_PATH() + "bs/admin/");

    build_service::proto::ServiceInfoRequest request;
    build_service::proto::ServiceInfoResponse response;

    ASSERT_TRUE(bsChannel->GetServiceInfo(request, response));
    fakeBs.reset();
    ASSERT_FALSE(bsChannel->GetServiceInfo(request, response));

    fakeBs = make_unique<FakeBsAdmin>();
    ASSERT_TRUE(fakeBs->init(0, nullptr));
    ASSERT_TRUE(fakeBs->startServer());

    ASSERT_FALSE(bsChannel->GetServiceInfo(request, response));
    WriteSpecFile(fakeBs->getIp(), fakeBs->getPort(), GET_TEMP_DATA_PATH() + "bs/admin/");
    ASSERT_TRUE(bsChannel->GetServiceInfo(request, response));

    build_service::proto::CreateSnapshotRequest req1;
    build_service::proto::InformResponse ret1;

    // test missing required fields
    ASSERT_FALSE(bsChannel->CreateSnapshot(req1, ret1));
    req1.set_clustername("cluster1");
    req1.set_versionid(3);
    *req1.mutable_buildid() = _pid.buildid();
    ASSERT_TRUE(bsChannel->CreateSnapshot(req1, ret1));

    build_service::proto::RemoveSnapshotRequest req2;
    build_service::proto::InformResponse ret2;

    // test missing required fields
    ASSERT_FALSE(bsChannel->RemoveSnapshot(req2, ret2));

    req2.set_clustername("cluster1");
    req2.set_versionid(3);
    *req2.mutable_buildid() = _pid.buildid();
    ASSERT_TRUE(bsChannel->RemoveSnapshot(req2, ret2));
}

TEST_F(CloneIndexTaskTest, testMadroxChannel)
{
    auto fakeMadrox = make_unique<FakeMadroxService>();
    ASSERT_TRUE(fakeMadrox->init(0, nullptr));
    ASSERT_TRUE(fakeMadrox->startServer());

    string madroxSpecRoot = fslib::util::FileUtil::joinFilePath(GET_TEMP_DATA_PATH(), "madrox");
    auto madroxChannel = make_shared<MadroxChannel>("zfs:///" + GET_TEMP_DATA_PATH() + "madrox/");
    ASSERT_TRUE(madroxChannel->init());
    WriteSpecFile(fakeMadrox->getIp(), fakeMadrox->getPort(), GET_TEMP_DATA_PATH() + "madrox/master/");

    ::madrox::proto::GetStatusRequest req1;
    ::madrox::proto::GetStatusResponse ret1;
    req1.add_destroot("pangu://dst1/");

    ASSERT_TRUE(madroxChannel->GetStatus(req1, ret1));
    fakeMadrox.reset();
    ASSERT_FALSE(madroxChannel->GetStatus(req1, ret1));

    fakeMadrox = make_unique<FakeMadroxService>();
    ASSERT_TRUE(fakeMadrox->init(0, nullptr));
    ASSERT_TRUE(fakeMadrox->startServer());

    ASSERT_FALSE(madroxChannel->GetStatus(req1, ret1));
    WriteSpecFile(fakeMadrox->getIp(), fakeMadrox->getPort(), GET_TEMP_DATA_PATH() + "madrox/master/");
    ASSERT_TRUE(madroxChannel->GetStatus(req1, ret1));

    ::madrox::proto::UpdateRequest req2;
    ::madrox::proto::UpdateResponse ret2;

    // test missing required fields
    ASSERT_FALSE(madroxChannel->UpdateRequest(req2, ret2));
    req2.set_srcroot("pangu://test/index");
    ASSERT_TRUE(madroxChannel->UpdateRequest(req2, ret2));
}

}} // namespace build_service::task_base
