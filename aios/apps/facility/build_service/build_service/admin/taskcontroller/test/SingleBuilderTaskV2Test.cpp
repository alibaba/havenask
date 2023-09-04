#include "build_service/admin/taskcontroller/SingleBuilderTaskV2.h"

#include <google/protobuf/util/json_util.h>
#include <memory>

#include "autil/EnvUtil.h"
#include "build_service/admin/ClusterCheckpointSynchronizerCreator.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/BrokerTopicAccessor.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/test/ProtoCreator.h"
#include "build_service/test/unittest.h"
#include "build_service/util/RangeUtil.h"
#include "indexlib/framework/Version.h"

using namespace std;
using namespace testing;
using namespace build_service::config;

namespace build_service { namespace admin {

class SingleBuilderTaskV2Test : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    void checkController(const SingleBuilderTaskV2& controller, const string& clusterName, uint32_t partitionCount,
                         uint32_t parallelNum, proto::BuildStep buildStep);
    void checkBuilders(vector<SingleBuilderTaskV2::BuilderGroup>, uint32_t partitionCount, uint32_t parallelNum);
    bool checkTarget(const TaskController::Nodes& nodes, uint32_t nodeId,
                     const proto::BuildTaskTargetInfo& expectTarget);

    void setCurrent(TaskController::Nodes& nodes, uint32_t nodeId, const proto::BuildTaskCurrentInfo& checkpoint,
                    bool isFinished);
    bool updateControllerConfig(SingleBuilderTaskV2& controller, const string& configPath);
    proto::BuildTaskCurrentInfo createCurrent(const string& fence, versionid_t versionId, uint64_t offset,
                                              bool hasAlignVersion);
    proto::BuildTaskCurrentInfo createCurrent(const std::string& indexRoot, const string& fence, versionid_t versionId,
                                              uint64_t offset, bool hasAlignVersion);
    pair<TaskController::Node, bool> getNode(const TaskController::Nodes& nodes, uint32_t nodeId);
    versionid_t getAlignVersionId(const SingleBuilderTaskV2& controller) { return controller._alignVersionId; }
    const std::map<std::string, versionid_t>& getRollbackVersionIdMapping(const SingleBuilderTaskV2& controller)
    {
        return controller._rollbackInfo.rollbackVersionIdMapping;
    }
    int64_t getCheckpointKeepCount(const SingleBuilderTaskV2& controller) { return controller._checkpointKeepCount; }
    common::ResourceKeeperGuardPtr getSwiftResourceGuard(const SingleBuilderTaskV2& controller)
    {
        return controller._swiftResourceGuard;
    }
    string getConfigPath(const SingleBuilderTaskV2& controller) { return controller._configPath; }
    proto::DataDescription getDataDescription(const SingleBuilderTaskV2& controller)
    {
        return controller._builderDataDesc;
    }
    vector<SingleBuilderTaskV2::BuilderGroup> getBuilders(const SingleBuilderTaskV2& controller)
    {
        return controller._builders;
    }
    SingleBuilderTaskV2 createController(const string& configPath);
    void checkJsonize(const SingleBuilderTaskV2& lhs, const SingleBuilderTaskV2& rhs);
    void checkClone(const SingleBuilderTaskV2& lhs, const SingleBuilderTaskV2& rhs);
    versionid_t getPublishedVersion(versionid_t versionId)
    {
        return versionId | indexlibv2::framework::Version::PUBLIC_VERSION_ID_MASK;
    }
    versionid_t getPrivateVersion(versionid_t versionId)
    {
        return versionId | indexlibv2::framework::Version::PRIVATE_VERSION_ID_MASK;
    }
    proto::Range getRange(uint32_t from, uint32_t to)
    {
        proto::Range range;
        range.set_from(from);
        range.set_to(to);
        return range;
    }
    std::string getVersionMetaStr(versionid_t versionId, int64_t ts)
    {
        indexlibv2::framework::VersionMeta versionMeta;
        versionMeta._versionId = versionId;
        versionMeta._timestamp = versionMeta._minLocatorTs = versionMeta._maxLocatorTs = ts;
        return autil::legacy::ToJsonString(versionMeta, /*isCompact=*/true);
    }
};

SingleBuilderTaskV2 SingleBuilderTaskV2Test::createController(const string& configPath)
{
    TaskResourceManagerPtr resourceManager = std::make_shared<TaskResourceManager>();
    ConfigReaderAccessorPtr configResource(new ConfigReaderAccessor("simple"));
    ResourceReaderPtr configReader = ResourceReaderManager::getResourceReader(configPath);
    configResource->addConfig(configReader, true);
    resourceManager->addResource(configResource);

    common::BrokerTopicAccessorPtr brokerTopicAccessor;
    proto::BuildId buildId = proto::ProtoCreator::createBuildId("simple", 1, "app");
    brokerTopicAccessor.reset(new common::BrokerTopicAccessor(buildId));
    resourceManager->addResource(brokerTopicAccessor);

    common::CheckpointAccessorPtr checkpointAccessor(new common::CheckpointAccessor);
    resourceManager->addResource(checkpointAccessor);

    return SingleBuilderTaskV2("", "", resourceManager);
}

void SingleBuilderTaskV2Test::checkController(const SingleBuilderTaskV2& controller, const string& clusterName,
                                              uint32_t partitionCount, uint32_t parallelNum, proto::BuildStep buildStep)
{
    ASSERT_EQ(clusterName, controller._clusterName);
    ASSERT_EQ(partitionCount, controller._partitionCount);
    ASSERT_EQ(parallelNum, controller._parallelNum);
    ASSERT_EQ(buildStep, controller._buildStep);
}

void SingleBuilderTaskV2Test::checkBuilders(vector<SingleBuilderTaskV2::BuilderGroup> builders, uint32_t partitionCount,
                                            uint32_t parallelNum)
{
    ASSERT_EQ(partitionCount, builders.size());
    for (const auto& builder : builders) {
        if (parallelNum == 1) {
            ASSERT_TRUE(builder.slaves.empty());
        } else {
            ASSERT_EQ(parallelNum, builder.slaves.size());
        }
    }
    auto masterRanges = util::RangeUtil::splitRange(0, 65535, partitionCount);
    auto slaveRanges = util::RangeUtil::splitRange(0, 65535, partitionCount, parallelNum);
    uint32_t masterIdx = 0;
    uint32_t slaveIdx = 0;
    for (const auto& builder : builders) {
        ASSERT_EQ(masterRanges[masterIdx], builder.master.range);
        masterIdx++;
        for (const auto& slave : builder.slaves) {
            ASSERT_EQ(slaveRanges[slaveIdx], slave.range);
            slaveIdx++;
        }
    }
}

bool SingleBuilderTaskV2Test::checkTarget(const TaskController::Nodes& nodes, uint32_t nodeId,
                                          const proto::BuildTaskTargetInfo& expectTarget)
{
    auto [node, exist] = getNode(nodes, nodeId);
    EXPECT_TRUE(exist);
    if (!exist) {
        return false;
    }
    string targetStr;
    EXPECT_TRUE(node.taskTarget.getTargetDescription(BS_BUILD_TASK_TARGET, targetStr));
    if (!node.taskTarget.getTargetDescription(BS_BUILD_TASK_TARGET, targetStr)) {
        return false;
    }
    string expectTargetStr = ToJsonString(expectTarget);
    EXPECT_EQ(expectTargetStr, targetStr);
    if (expectTargetStr != targetStr) {
        return false;
    }
    return true;
}

void SingleBuilderTaskV2Test::setCurrent(TaskController::Nodes& nodes, uint32_t nodeId,
                                         const proto::BuildTaskCurrentInfo& checkpoint, bool isFinished)
{
    auto [node, exist] = getNode(nodes, nodeId);
    ASSERT_TRUE(exist);
    node.statusDescription = ToJsonString(checkpoint);
    node.reachedTarget = isFinished;
    for (auto& rawNode : nodes) {
        if (rawNode.nodeId == nodeId) {
            rawNode = node;
        }
    }
}

proto::BuildTaskCurrentInfo SingleBuilderTaskV2Test::createCurrent(const string& fence, versionid_t versionId,
                                                                   uint64_t offset, bool hasAlignVersion)
{
    proto::BuildTaskCurrentInfo currentInfo;
    indexlibv2::framework::VersionMeta versionMeta;
    versionMeta._fenceName = fence;
    versionMeta._versionId = versionId;
    versionMeta._minLocatorTs = offset;
    versionMeta._maxLocatorTs = offset;
    currentInfo.commitedVersion.versionMeta = versionMeta;
    if (hasAlignVersion) {
        currentInfo.lastAlignedVersion = currentInfo.commitedVersion;
    }
    return currentInfo;
}

proto::BuildTaskCurrentInfo SingleBuilderTaskV2Test::createCurrent(const std::string& indexRoot, const string& fence,
                                                                   versionid_t versionId, uint64_t offset,
                                                                   bool hasAlignVersion)
{
    proto::BuildTaskCurrentInfo currentInfo = createCurrent(fence, versionId, offset, hasAlignVersion);
    currentInfo.commitedVersion.indexRoot = indexRoot;
    return currentInfo;
}

pair<TaskController::Node, bool> SingleBuilderTaskV2Test::getNode(const TaskController::Nodes& nodes, uint32_t nodeId)
{
    for (const auto& node : nodes) {
        if (node.nodeId == nodeId) {
            return {node, true};
        }
    }
    return {TaskController::Node(), false};
}

bool SingleBuilderTaskV2Test::updateControllerConfig(SingleBuilderTaskV2& controller, const string& configPath)
{
    ConfigReaderAccessorPtr configResource(new ConfigReaderAccessor("simple"));
    ResourceReaderPtr configReader = ResourceReaderManager::getResourceReader(configPath);
    configResource->addConfig(configReader, true);
    controller.TaskController::_resourceManager->addResourceIgnoreExist(configResource);
    return controller.updateConfig();
}

void SingleBuilderTaskV2Test::checkJsonize(const SingleBuilderTaskV2& lhs, const SingleBuilderTaskV2& rhs)
{
    ASSERT_EQ(lhs._clusterName, rhs._clusterName);
    ASSERT_EQ(lhs._partitionCount, rhs._partitionCount);
    ASSERT_EQ(lhs._parallelNum, rhs._parallelNum);
    ASSERT_EQ(lhs._configPath, rhs._configPath);
    ASSERT_EQ(lhs._buildStep, rhs._buildStep);
    ASSERT_EQ(lhs._builderDataDesc, rhs._builderDataDesc);
    ASSERT_EQ(lhs._alignVersionId, rhs._alignVersionId);
    ASSERT_EQ(lhs._lastAlignVersionTimestamp, rhs._lastAlignVersionTimestamp);
    ASSERT_EQ(lhs._checkpointKeepCount, rhs._checkpointKeepCount);
}
void SingleBuilderTaskV2Test::checkClone(const SingleBuilderTaskV2& lhs, const SingleBuilderTaskV2& rhs)
{
    ASSERT_EQ(lhs._clusterName, rhs._clusterName);
    ASSERT_EQ(lhs._partitionCount, rhs._partitionCount);
    ASSERT_EQ(lhs._parallelNum, rhs._parallelNum);
    ASSERT_EQ(lhs._alignVersionId, rhs._alignVersionId);
    ASSERT_EQ(lhs._lastAlignVersionTimestamp, rhs._lastAlignVersionTimestamp);
    ASSERT_EQ(lhs._checkpointKeepCount, rhs._checkpointKeepCount);
}
void SingleBuilderTaskV2Test::setUp() {}
void SingleBuilderTaskV2Test::tearDown() {}

TEST_F(SingleBuilderTaskV2Test, testFullBuild)
{
    KeyValueMap param;
    param["buildStep"] = "full";
    string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    SingleBuilderTaskV2 controller = createController(configPath);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));

    // start build full with 2 part, 3 parallel
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 3;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);

    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(8u, nodes.size());

    proto::DataDescription dataDescription;
    dataDescription["src"] = "swift";
    dataDescription["type"] = "swift_processed_topic";
    dataDescription["swift_root"] = "zfs://root/";
    dataDescription["swift_start_timestamp"] = "0";
    dataDescription["topic_name"] = "user_name_service_name_processed_1_cluster1";
    dataDescription["src_signature"] = std::to_string(SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
    for (const auto& node : nodes) {
        std::string dataDescStr;
        node.taskTarget.getTargetDescription(DATA_DESCRIPTION_KEY, dataDescStr);
        ASSERT_EQ(ToJsonString(dataDescription), dataDescStr);
    }
    // master nodeId: 0,4  check master target
    proto::BuildTaskTargetInfo initMasterTarget;
    initMasterTarget.finished = false;
    initMasterTarget.buildMode = proto::BuildMode::PUBLISH;
    initMasterTarget.alignVersionId = indexlibv2::INVALID_VERSIONID;
    initMasterTarget.buildStep = "full";
    initMasterTarget.latestPublishedVersion = indexlibv2::INVALID_VERSIONID;
    initMasterTarget.indexRoot = "";
    initMasterTarget.slaveVersionProgress = {};
    vector<uint32_t> masterNodeIds = {0, 4};
    for (uint32_t nodeId : masterNodeIds) {
        ASSERT_TRUE(checkTarget(nodes, nodeId, initMasterTarget));
    }

    // slave nodeId: 1,2,3,5,6,7 check slave target
    proto::BuildTaskTargetInfo initSlaveTarget = initMasterTarget;
    initSlaveTarget.buildMode = proto::BuildMode::BUILD;
    vector<uint32_t> slaveNodeIds = {1, 2, 3, 5, 6, 7};
    for (uint32_t nodeId : slaveNodeIds) {
        ASSERT_TRUE(checkTarget(nodes, nodeId, initSlaveTarget));
    }

    // set current, slave builders start build
    for (uint32_t nodeId : slaveNodeIds) {
        setCurrent(nodes, nodeId,
                   createCurrent(/*indexRoot*/ "indexRoot_" + to_string(nodeId), /*fence*/ "fence_" + to_string(nodeId),
                                 /*versionId*/ getPublishedVersion(1), /*offset*/ 1,
                                 /*hasAlignVersion*/ false),
                   /*isFinished*/ false);
    }
    controller.operate(nodes);

    // check master builders target with slave versionProgress
    proto::BuildTaskTargetInfo target = initMasterTarget;
    target.slaveVersionProgress = {proto::VersionProgress("fence_1", getPublishedVersion(1)),
                                   proto::VersionProgress("fence_2", getPublishedVersion(1)),
                                   proto::VersionProgress("fence_3", getPublishedVersion(1))};
    ASSERT_TRUE(checkTarget(nodes, 0, target));
    target.slaveVersionProgress = {proto::VersionProgress("fence_5", getPublishedVersion(1)),
                                   proto::VersionProgress("fence_6", getPublishedVersion(1)),
                                   proto::VersionProgress("fence_7", getPublishedVersion(1))};
    ASSERT_TRUE(checkTarget(nodes, 4, target));

    // set current, master builder import version & publich
    setCurrent(nodes, 0,
               createCurrent(/*indexRoot*/ "indexRoot", /*fence*/ "", /*versionId*/ getPublishedVersion(100),
                             /*offset*/ 1,
                             /*hasAlignVersion*/ false),
               /*isFinished*/ false);
    setCurrent(nodes, 4,
               createCurrent(/*indexRoot*/ "indexRoot", /*fence*/ "", /*versionId*/ getPublishedVersion(100),
                             /*offset*/ 1,
                             /*hasAlignVersion*/ false),
               /*isFinished*/ false);
    controller.operate(nodes);

    // check master builders target with latestPublishedVersion and indexRoot
    target.latestPublishedVersion = getPublishedVersion(100);
    target.indexRoot = "indexRoot";
    ASSERT_TRUE(checkTarget(nodes, 4, target));

    // check slave builders target with latestPublishedVersion
    target = initSlaveTarget;
    target.latestPublishedVersion = getPublishedVersion(100);
    for (uint32_t nodeId : slaveNodeIds) {
        target.slaveVersionProgress = {proto::VersionProgress("fence_" + to_string(nodeId), getPublishedVersion(1))};
        ASSERT_TRUE(checkTarget(nodes, nodeId, target));
    }

    // set current , slave builders finish
    for (uint32_t nodeId : slaveNodeIds) {
        setCurrent(nodes, nodeId,
                   createCurrent(/*fence*/ "fence_" + to_string(nodeId), /*versionId*/ getPublishedVersion(2),
                                 /*offset*/ 3,
                                 /*hasAlignVersion*/ false),
                   /*isFinished*/ true);
    }
    controller.operate(nodes);

    for (uint32_t nodeId : slaveNodeIds) {
        auto [node, exist] = getNode(nodes, nodeId);
        ASSERT_TRUE(exist);
        ASSERT_TRUE(node.reachedTarget);
    }
}

TEST_F(SingleBuilderTaskV2Test, testFullBuildAlignVersion)
{
    KeyValueMap param;
    param["buildStep"] = "full";
    string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    SingleBuilderTaskV2 controller = createController(configPath);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));
    // start build full with 2 part, 3 parallel
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 3;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);

    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(8u, nodes.size());

    vector<uint32_t> masterNodeIds = {0, 4};
    vector<uint32_t> slaveNodeIds = {1, 2, 3, 5, 6, 7};
    proto::DataDescription dataDescription;
    dataDescription["src"] = "swift";
    dataDescription["type"] = "swift_processed_topic";
    dataDescription["swift_root"] = "zfs://root/";
    dataDescription["swift_start_timestamp"] = "0";
    dataDescription["topic_name"] = "user_name_service_name_processed_1_cluster1";
    dataDescription["src_signature"] = std::to_string(SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
    for (const auto& node : nodes) {
        std::string dataDescStr;
        node.taskTarget.getTargetDescription(DATA_DESCRIPTION_KEY, dataDescStr);
        ASSERT_EQ(ToJsonString(dataDescription), dataDescStr);
    }
    // all slave reached target, trigger alignVersion
    proto::BuildTaskCurrentInfo current;

    for (uint32_t nodeId : slaveNodeIds) {
        setCurrent(nodes, nodeId, current, /*isFinished*/ true);
    }

    setCurrent(nodes, 0,
               createCurrent(/*indexRoot*/ "indexRoot0", /*fence*/ "", /*versionId*/ getPublishedVersion(4),
                             /*offset*/ 3,
                             /*hasAlignVersion*/ false),
               /*isFinished*/ false);
    setCurrent(nodes, 4,
               createCurrent(/*indexRoot*/ "indexRoot4", /*fence*/ "", /*versionId*/ getPublishedVersion(4),
                             /*offset*/ 3,
                             /*hasAlignVersion*/ false),
               /*isFinished*/ false);

    // triggerAlignVersion,  max(4,4) + 4 = 8, set _AlignVersion, target alignVersion not set
    controller.operate(nodes);
    ASSERT_EQ(getPublishedVersion(8), getAlignVersionId(controller));

    // set target alignVersion
    controller.operate(nodes);

    proto::BuildTaskTargetInfo target;
    target.finished = true; // all slave finished && alignVersion != -1
    target.buildMode = proto::BuildMode::PUBLISH;
    target.alignVersionId = getPublishedVersion(8);
    target.buildStep = "full";
    target.latestPublishedVersion = getPublishedVersion(4);
    target.indexRoot = "indexRoot0";
    target.slaveVersionProgress = {};

    ASSERT_TRUE(checkTarget(nodes, 0, target));
    target.indexRoot = "indexRoot4";
    ASSERT_TRUE(checkTarget(nodes, 4, target));

    // master builder 0 finish align version
    setCurrent(nodes, 0,
               createCurrent(/*indexRoot*/ "indexRoot0", /*fence*/ "", /*versionId*/ getPublishedVersion(8),
                             /*offset*/ 3,
                             /*hasAlignVersion*/ true),
               /*isFinished*/ true);
    controller.operate(nodes);
    for (uint32_t nodeId : masterNodeIds) {
        auto [node, exist] = getNode(nodes, nodeId);
        ASSERT_TRUE(exist);
        ASSERT_TRUE(!node.reachedTarget);
    }
    // all finished
    setCurrent(nodes, 0,
               createCurrent(/*indexRoot*/ "indexRoot0", /*fence*/ "", /*versionId*/ getPublishedVersion(8),
                             /*offset*/ 3,
                             /*hasAlignVersion*/ true),
               /*isFinished*/ true);
    setCurrent(nodes, 4,
               createCurrent(/*indexRoot*/ "indexRoot44", /*fence*/ "", /*versionId*/ getPublishedVersion(8),
                             /*offset*/ 3,
                             /*hasAlignVersion*/ true),
               /*isFinished*/ true);
    controller.operate(nodes);
    target.indexRoot = "indexRoot44";
    target.latestPublishedVersion = getPublishedVersion(8);
    ASSERT_TRUE(checkTarget(nodes, 4, target));
    for (uint32_t nodeId : masterNodeIds) {
        auto [node, exist] = getNode(nodes, nodeId);
        ASSERT_TRUE(exist);
        ASSERT_TRUE(node.reachedTarget);
    }
}

TEST_F(SingleBuilderTaskV2Test, testIncBuildAlignVersion)
{
    KeyValueMap param;
    param["buildStep"] = "increment";
    string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    SingleBuilderTaskV2 controller = createController(configPath);
    std::shared_ptr<common::CheckpointAccessor> checkpointAccessor;
    controller._resourceManager->getResource(checkpointAccessor);
    proto::BuildId buildId = proto::ProtoCreator::createBuildId("simple", 1, "app");
    ClusterCheckpointSynchronizerCreator creator(buildId);
    auto clusterCheckpointSyncs =
        creator.create(checkpointAccessor, configPath, /*metricsReporter=*/nullptr, GET_TEST_DATA_PATH(),
                       ClusterCheckpointSynchronizerCreator::Type::DEFAULT);
    ASSERT_EQ(clusterCheckpointSyncs.size(), 1u);
    auto checkpointSync = std::make_shared<CheckpointSynchronizer>(buildId);
    ASSERT_TRUE(checkpointSync->init(clusterCheckpointSyncs, /*isLeaderFollowerMode=*/false));
    controller._resourceManager->addResource(checkpointSync);

    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));

    // start build full with 2 part, 1 parallel
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 1;

    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_INC);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);

    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(2u, nodes.size());

    proto::DataDescription dataDescription;
    dataDescription["src"] = "swift";
    dataDescription["type"] = "swift_processed_topic";
    dataDescription["swift_root"] = "zfs://root/";
    dataDescription["swift_start_timestamp"] = "0";
    dataDescription["topic_name"] = "user_name_service_name_processed_1_cluster1";
    dataDescription["src_signature"] = std::to_string(SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
    for (const auto& node : nodes) {
        std::string dataDescStr;
        node.taskTarget.getTargetDescription(DATA_DESCRIPTION_KEY, dataDescStr);
        ASSERT_EQ(ToJsonString(dataDescription), dataDescStr);
    }

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(4), /*offset*/ 3, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(2), /*offset*/ 3, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    autil::EnvUtil::setEnv(std::string("bs_version_publish_interval_sec"), "0");
    controller.operate(nodes);
    ASSERT_EQ(getPublishedVersion(8), getAlignVersionId(controller));
    autil::EnvUtil::unsetEnv(std::string("bs_version_publish_interval_sec"));
    // set master target alignVersionId
    controller.operate(nodes);

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(8), /*offset*/ 5, /*hasAlignVersion*/ true),
        /*isFinished*/ false);
    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(8), /*offset*/ 5, /*hasAlignVersion*/ true),
        /*isFinished*/ false);
    controller.operate(nodes);
    ASSERT_EQ(indexlibv2::INVALID_VERSIONID, getAlignVersionId(controller));
    autil::EnvUtil::setEnv(std::string("bs_version_publish_interval_sec"), "0");
    controller.operate(nodes);
    // no new version, don't need align
    ASSERT_EQ(indexlibv2::INVALID_VERSIONID, getAlignVersionId(controller));
    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(9), /*offset*/ 6, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    controller.operate(nodes);
    ASSERT_EQ(getPublishedVersion(13), getAlignVersionId(controller));

    controller.operate(nodes);
    proto::BuildTaskTargetInfo target;
    target.finished = false;
    target.buildMode = proto::BuildMode::BUILD_PUBLISH;
    target.alignVersionId = getPublishedVersion(13);
    target.buildStep = "incremental";
    target.latestPublishedVersion = getPublishedVersion(9);
    ASSERT_TRUE(checkTarget(nodes, 0, target));
    target.latestPublishedVersion = getPublishedVersion(8);
    ASSERT_TRUE(checkTarget(nodes, 1, target));

    autil::EnvUtil::unsetEnv(std::string("bs_version_publish_interval_sec"));
}

TEST_F(SingleBuilderTaskV2Test, testUpdateConfig)
{
    KeyValueMap param;
    param["buildStep"] = "full";
    string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    SingleBuilderTaskV2 controller = createController(configPath);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));

    // start build full with 2 part, 3 paralle
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 3;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);

    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(8u, nodes.size());

    proto::DataDescription dataDescription;
    dataDescription["src"] = "swift";
    dataDescription["type"] = "swift_processed_topic";
    dataDescription["swift_root"] = "zfs://root/";
    dataDescription["swift_start_timestamp"] = "0";
    dataDescription["topic_name"] = "user_name_service_name_processed_1_cluster1";
    dataDescription["src_signature"] = std::to_string(SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
    for (const auto& node : nodes) {
        std::string dataDescStr;
        node.taskTarget.getTargetDescription(DATA_DESCRIPTION_KEY, dataDescStr);
        ASSERT_EQ(ToJsonString(dataDescription), dataDescStr);
    }

    setCurrent(nodes, 1,
               createCurrent(/*fence*/ "fence_1", /*versionId*/ getPublishedVersion(1), /*offset*/ 6,
                             /*hasAlignVersion*/ false),
               /*isFinished*/ false);
    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(100), /*offset*/ 6, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    setCurrent(
        nodes, 4,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(200), /*offset*/ 6, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    controller.operate(nodes);

    proto::BuildTaskTargetInfo initTarget;
    proto::BuildTaskTargetInfo target;
    initTarget.finished = false;
    initTarget.buildMode = proto::BuildMode::UNKOWN;
    initTarget.alignVersionId = indexlibv2::INVALID_VERSIONID;
    initTarget.buildStep = "full";
    initTarget.latestPublishedVersion = indexlibv2::INVALID_VERSIONID;
    initTarget.slaveVersionProgress = {};

    target = initTarget;
    target.buildMode = proto::BuildMode::BUILD;
    target.latestPublishedVersion = getPublishedVersion(100);
    target.slaveVersionProgress = {proto::VersionProgress("fence_1", getPublishedVersion(1))};
    ASSERT_TRUE(checkTarget(nodes, 1, target));
    target.buildMode = proto::BuildMode::PUBLISH;
    ASSERT_TRUE(checkTarget(nodes, 0, target));

    target = initTarget;
    target.buildMode = proto::BuildMode::PUBLISH;
    target.latestPublishedVersion = getPublishedVersion(200);
    ASSERT_TRUE(checkTarget(nodes, 4, target));

    setCurrent(nodes, 1,
               createCurrent(/*fence*/ "fence_1", /*versionId*/ getPublishedVersion(1), /*offset*/ 6,
                             /*hasAlignVersion*/ false),
               /*isFinished*/ false);
    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(100), /*offset*/ 6, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    setCurrent(
        nodes, 4,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(200), /*offset*/ 6, /*hasAlignVersion*/ false),
        /*isFinished*/ false);

    // update config, partNum 2, parallelNum 1, fullBuild
    std::string configPath1 = GET_TEST_DATA_PATH() + "admin_test/configV2_updateParallel";
    ASSERT_TRUE(updateControllerConfig(controller, configPath1));
    parallelNum = 1;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    ASSERT_EQ(10, getCheckpointKeepCount(controller));

    controller.operate(nodes);
    ASSERT_EQ(2u, nodes.size());

    // master builder recover
    target = initTarget;
    target.latestPublishedVersion = getPublishedVersion(100);
    target.buildMode = proto::BuildMode::BUILD_PUBLISH;
    ASSERT_TRUE(checkTarget(nodes, 0, target));
    target.latestPublishedVersion = getPublishedVersion(200);
    ASSERT_TRUE(checkTarget(nodes, 1, target));

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(400), /*offset*/ 8, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(400), /*offset*/ 8, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    controller.operate(nodes);
    target.latestPublishedVersion = getPublishedVersion(400);
    ASSERT_TRUE(checkTarget(nodes, 0, target));
    ASSERT_TRUE(checkTarget(nodes, 1, target));

    std::string rawConfigPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    ASSERT_TRUE(updateControllerConfig(controller, rawConfigPath));
    parallelNum = 3;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    ASSERT_EQ(10, getCheckpointKeepCount(controller));
    controller.operate(nodes);
    ASSERT_EQ(8u, nodes.size());

    // master builder recorver
    target = initTarget;
    target.latestPublishedVersion = getPublishedVersion(400);
    target.buildMode = proto::BuildMode::PUBLISH;
    ASSERT_TRUE(checkTarget(nodes, 0, target));
    ASSERT_TRUE(checkTarget(nodes, 4, target));
}

TEST_F(SingleBuilderTaskV2Test, testFullBuildWithBuildPublishMode)
{
    // full build with BUILD_PUBLISH (parallel num = 1)
    KeyValueMap param;
    param["buildStep"] = "full";
    std::string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2_updateParallel";
    SingleBuilderTaskV2 controller = createController(configPath);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 1;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    ASSERT_EQ(10, getCheckpointKeepCount(controller));

    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(2u, nodes.size());

    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(1), /*offset*/ 6, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(2), /*offset*/ 6, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    controller.operate(nodes);

    proto::BuildTaskTargetInfo initTarget;
    initTarget.finished = false;
    initTarget.buildMode = proto::BuildMode::UNKOWN;
    initTarget.alignVersionId = indexlibv2::INVALID_VERSIONID;
    initTarget.buildStep = "full";
    initTarget.latestPublishedVersion = indexlibv2::INVALID_VERSIONID;
    initTarget.slaveVersionProgress = {};

    proto::BuildTaskTargetInfo target = initTarget;
    target.buildMode = proto::BuildMode::BUILD_PUBLISH;
    target.latestPublishedVersion = getPublishedVersion(1);
    target.slaveVersionProgress = {};
    ASSERT_TRUE(checkTarget(nodes, 1, target));

    target.latestPublishedVersion = getPublishedVersion(2);
    target.slaveVersionProgress = {};
    ASSERT_TRUE(checkTarget(nodes, 0, target));

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(3), /*offset*/ 8, /*hasAlignVersion*/ false),
        /*isFinished*/ true);
    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(4), /*offset*/ 8, /*hasAlignVersion*/ false),
        /*isFinished*/ true);
    controller.operate(nodes);
    for (const auto& node : nodes) {
        ASSERT_TRUE(!node.reachedTarget);
    }
    ASSERT_EQ(getPublishedVersion(8), getAlignVersionId(controller));
    controller.operate(nodes);

    target.finished = true;
    target.latestPublishedVersion = getPublishedVersion(3);
    target.slaveVersionProgress = {};
    target.alignVersionId = getPublishedVersion(8);
    ASSERT_TRUE(checkTarget(nodes, 0, target));
    target.latestPublishedVersion = getPublishedVersion(4);
    ASSERT_TRUE(checkTarget(nodes, 1, target));

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(8), /*offset*/ 8, /*hasAlignVersion*/ true),
        /*isFinished*/ true);
    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(4), /*offset*/ 8, /*hasAlignVersion*/ false),
        /*isFinished*/ true);
    controller.operate(nodes);
    target.latestPublishedVersion = getPublishedVersion(8);
    ASSERT_TRUE(checkTarget(nodes, 0, target));
    target.latestPublishedVersion = getPublishedVersion(4);
    ASSERT_TRUE(checkTarget(nodes, 1, target));
    for (const auto& node : nodes) {
        ASSERT_TRUE(!node.reachedTarget);
    }

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(8), /*offset*/ 8, /*hasAlignVersion*/ true),
        /*isFinished*/ true);
    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(8), /*offset*/ 8, /*hasAlignVersion*/ true),
        /*isFinished*/ true);
    controller.operate(nodes);

    for (const auto& node : nodes) {
        ASSERT_TRUE(node.reachedTarget);
    }
}

TEST_F(SingleBuilderTaskV2Test, testFinish)
{
    KeyValueMap param;
    param["buildStep"] = "full";
    std::string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    SingleBuilderTaskV2 controller = createController(configPath);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 3;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);
    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(8u, nodes.size());

    proto::DataDescription dataDescription;
    dataDescription["src"] = "swift";
    dataDescription["type"] = "swift_processed_topic";
    dataDescription["swift_root"] = "zfs://root/";
    dataDescription["swift_start_timestamp"] = "0";
    dataDescription["topic_name"] = "user_name_service_name_processed_1_cluster1";
    dataDescription["src_signature"] = std::to_string(SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
    for (const auto& node : nodes) {
        std::string dataDescStr;
        node.taskTarget.getTargetDescription(DATA_DESCRIPTION_KEY, dataDescStr);
        ASSERT_EQ(ToJsonString(dataDescription), dataDescStr);
    }
    // finish
    KeyValueMap finishParam;
    ASSERT_TRUE(controller.finish(finishParam));
    proto::DataDescription ds = getDataDescription(controller);
    ASSERT_TRUE(ds.find("stopTimestamp") != ds.end());
    dataDescription["stopTimestamp"] = ds["stopTimestamp"];
    ASSERT_EQ(ToJsonString(dataDescription), ToJsonString(ds));
}

TEST_F(SingleBuilderTaskV2Test, testIncBuildAlignVersionFailOver)
{
    KeyValueMap param;
    param["buildStep"] = "increment";
    string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    SingleBuilderTaskV2 controller = createController(configPath);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));

    // start build full with 2 part, 1 paralle
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 1;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_INC);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);

    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(2u, nodes.size());

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(4), /*offset*/ 3, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(2), /*offset*/ 3, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    autil::EnvUtil::setEnv(std::string("bs_version_publish_interval_sec"), "0");
    controller.operate(nodes);
    ASSERT_EQ(getPublishedVersion(8), getAlignVersionId(controller));
    autil::EnvUtil::unsetEnv(std::string("bs_version_publish_interval_sec"));
    // set master target alignVersionId
    controller.operate(nodes);

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(4), /*offset*/ 5, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(8), /*offset*/ 5, /*hasAlignVersion*/ true),
        /*isFinished*/ false);
    controller.operate(nodes);
    ASSERT_EQ(getPublishedVersion(8), getAlignVersionId(controller));

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(4), /*offset*/ 5, /*hasAlignVersion*/ false),
        /*isFinished*/ false);

    // builder 1 failover, commited version invalid, version exist, align failed
    auto current = createCurrent(/*fence*/ "", /*versionId*/ indexlibv2::INVALID_VERSIONID, /*offset*/ 0,
                                 /*hasAlignVersion*/ false);
    current.lastAlignFailedVersionId = getPublishedVersion(6);
    setCurrent(nodes, 1, current, /*isFinished*/ false);
    controller.operate(nodes);
    ASSERT_EQ(getPublishedVersion(8), getAlignVersionId(controller));
}

TEST_F(SingleBuilderTaskV2Test, testGetTaskInfo)
{
    KeyValueMap param;
    param["buildStep"] = "full";
    std::string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    SingleBuilderTaskV2 controller = createController(configPath);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 3;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);
    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(8u, nodes.size());

    string taskInfoStr = controller.getTaskInfo();
    proto::SingleClusterInfo taskInfo;
    ASSERT_TRUE(google::protobuf::util::JsonStringToMessage(taskInfoStr, &taskInfo).ok());
    /*{"clusterName":"cluster1",
      "partitionCount":2,
      "clusterStep":"full_building",
      "builderInfo":{
          "parallelNum":3,
          "buildPhrase":"build"
      }}*/
    ASSERT_EQ("cluster1", taskInfo.clustername());
    ASSERT_EQ(2, taskInfo.partitioncount());
    ASSERT_EQ("full_building", taskInfo.clusterstep());
    proto::BuilderInfo builderInfo = taskInfo.builderinfo();
    ASSERT_EQ(3, builderInfo.parallelnum());
    ASSERT_EQ("build", builderInfo.buildphrase());

    // update config
    std::string configPath1 = GET_TEST_DATA_PATH() + "admin_test/configV2_updateParallel";
    ASSERT_TRUE(updateControllerConfig(controller, configPath1));
    taskInfoStr = controller.getTaskInfo();
    ASSERT_TRUE(google::protobuf::util::JsonStringToMessage(taskInfoStr, &taskInfo).ok());

    ASSERT_EQ("cluster1", taskInfo.clustername());
    ASSERT_EQ(2, taskInfo.partitioncount());
    ASSERT_EQ("full_building", taskInfo.clusterstep());
    builderInfo = taskInfo.builderinfo();
    ASSERT_EQ(1, builderInfo.parallelnum());
    ASSERT_EQ("build", builderInfo.buildphrase());

    KeyValueMap finishParam;
    ASSERT_TRUE(controller.finish(finishParam));
    taskInfoStr = controller.getTaskInfo();
    ASSERT_TRUE(google::protobuf::util::JsonStringToMessage(taskInfoStr, &taskInfo).ok());

    ASSERT_EQ("cluster1", taskInfo.clustername());
    ASSERT_EQ(2, taskInfo.partitioncount());
    // full stopping
    ASSERT_EQ("full_stopping", taskInfo.clusterstep());
    builderInfo = taskInfo.builderinfo();
    ASSERT_EQ(1, builderInfo.parallelnum());
    ASSERT_EQ("build", builderInfo.buildphrase());
}

TEST_F(SingleBuilderTaskV2Test, testClone)
{
    KeyValueMap param;
    param["buildStep"] = "full";
    std::string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    SingleBuilderTaskV2 controller = createController(configPath);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 3;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);
    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(8u, nodes.size());

    SingleBuilderTaskV2* clonedController = (SingleBuilderTaskV2*)(controller.clone());
    checkController(*clonedController, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    checkClone(controller, *clonedController);
}

TEST_F(SingleBuilderTaskV2Test, testJsonize)
{
    KeyValueMap param;
    param["buildStep"] = "full";
    std::string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    SingleBuilderTaskV2 controller = createController(configPath);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));
    ASSERT_TRUE(getSwiftResourceGuard(controller) != nullptr);

    // start build full with 2 part, 3 parallel
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 3;
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_FULL);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);

    string str = ToJsonString(controller);
    SingleBuilderTaskV2 controller2 = createController(configPath);
    FromJsonString(controller2, str);
    checkJsonize(controller, controller2);
    ASSERT_FALSE(getSwiftResourceGuard(controller2) != nullptr);
    TaskController::Nodes nodes;
    ASSERT_FALSE(controller2.operate(nodes));
    ASSERT_TRUE(getSwiftResourceGuard(controller2) != nullptr);
}

TEST_F(SingleBuilderTaskV2Test, testRollingBack)
{
    KeyValueMap param;
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 1;
    param["buildStep"] = "increment";
    string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    SingleBuilderTaskV2 controller = createController(configPath);
    std::shared_ptr<common::CheckpointAccessor> checkpointAccessor;
    controller._resourceManager->getResource(checkpointAccessor);
    proto::BuildId buildId = proto::ProtoCreator::createBuildId("simple", 1, "app");
    ClusterCheckpointSynchronizerCreator creator(buildId);
    auto clusterCheckpointSyncs =
        creator.create(checkpointAccessor, configPath, /*metricsReporter=*/nullptr, GET_TEST_DATA_PATH(),
                       ClusterCheckpointSynchronizerCreator::Type::DEFAULT);
    auto checkpointSync = std::make_shared<CheckpointSynchronizer>(buildId);
    checkpointSync->init(clusterCheckpointSyncs, /*isLeaderFollowerMode=*/false);
    std::string errMsg;
    ASSERT_TRUE(checkpointSync->publishPartitionLevelCheckpoint(
        "cluster1", getRange(0, 32767), getVersionMetaStr(getPublishedVersion(1), /*ts=*/1), errMsg));
    ASSERT_TRUE(checkpointSync->publishPartitionLevelCheckpoint(
        "cluster1", getRange(32768, 65535), getVersionMetaStr(getPublishedVersion(1), /*ts=*/1), errMsg));
    ClusterCheckpointSynchronizer::SyncOptions syncOptions;
    syncOptions.isDaily = false;
    syncOptions.isInstant = true;
    syncOptions.isOffline = true;
    syncOptions.addIndexInfos = true;
    ASSERT_TRUE(checkpointSync->syncCluster("cluster1", syncOptions));
    ::google::protobuf::RepeatedPtrField<proto::Range> ranges;
    checkpointid_t newCheckpointId;
    ASSERT_TRUE(checkpointSync->rollback("cluster1", 0, ranges, /*needAddIndexInfo=*/false, &newCheckpointId, errMsg));
    ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult result;
    checkpointSync->createSavepoint("cluster1", newCheckpointId, "", &result, errMsg);
    controller._resourceManager->addResource(checkpointSync);

    param[BS_ROLLBACK_SOURCE_CHECKPOINT] = std::to_string(newCheckpointId);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));

    // start build full with 2 part, 1 parallel
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_INC);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);

    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(2u, nodes.size());

    proto::DataDescription dataDescription;
    dataDescription["src"] = "swift";
    dataDescription["type"] = "swift_processed_topic";
    dataDescription["swift_root"] = "zfs://root/";
    dataDescription["swift_start_timestamp"] = "0";
    dataDescription["topic_name"] = "user_name_service_name_processed_1_cluster1";
    dataDescription["src_signature"] = std::to_string(SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
    proto::BuildTaskTargetInfo target;
    target.finished = false;
    target.buildMode = proto::BuildMode::BUILD_PUBLISH;
    target.alignVersionId = indexlibv2::INVALID_VERSIONID;
    target.buildStep = "incremental";
    target.latestPublishedVersion = getPublishedVersion(1);
    target.indexRoot = "";
    target.slaveVersionProgress = {};
    target.branchId = 1;
    checkTarget(nodes, 0, target);
    checkTarget(nodes, 1, target);

    for (const auto& node : nodes) {
        std::string dataDescStr;
        node.taskTarget.getTargetDescription(DATA_DESCRIPTION_KEY, dataDescStr);
        ASSERT_EQ(ToJsonString(dataDescription), dataDescStr);
    }

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(3), /*offset*/ 3, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(4), /*offset*/ 3, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    autil::EnvUtil::setEnv(std::string("bs_version_publish_interval_sec"), "0");
    controller.operate(nodes);
    ASSERT_EQ(getPublishedVersion(8), getAlignVersionId(controller));
    autil::EnvUtil::unsetEnv(std::string("bs_version_publish_interval_sec"));
    // set master target alignVersionId
    const auto& rollbackVersionIdMapping = getRollbackVersionIdMapping(controller);
    ASSERT_EQ(2, rollbackVersionIdMapping.size());
    ASSERT_EQ(getPublishedVersion(1), rollbackVersionIdMapping.begin()->second);
    ASSERT_EQ(getPublishedVersion(1), rollbackVersionIdMapping.rbegin()->second);
    controller.operate(nodes);

    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(8), /*offset*/ 5, /*hasAlignVersion*/ true),
        /*isFinished*/ false);
    setCurrent(
        nodes, 1,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(8), /*offset*/ 5, /*hasAlignVersion*/ true),
        /*isFinished*/ false);
    controller.operate(nodes);

    target.finished = false;
    target.buildMode = proto::BuildMode::BUILD_PUBLISH;
    target.alignVersionId = getPublishedVersion(8);
    target.buildStep = "incremental";
    target.latestPublishedVersion = getPublishedVersion(8);
    target.indexRoot = "";
    target.slaveVersionProgress = {};
    target.branchId = 1;
    checkTarget(nodes, 0, target);
    checkTarget(nodes, 1, target);
    ASSERT_EQ(indexlibv2::INVALID_VERSIONID, getAlignVersionId(controller));
    ASSERT_EQ(0, getRollbackVersionIdMapping(controller).size());

    controller.operate(nodes);
    target.alignVersionId = indexlibv2::INVALID_VERSIONID;
    checkTarget(nodes, 0, target);
    checkTarget(nodes, 1, target);
}

TEST_F(SingleBuilderTaskV2Test, testRollingBackWithSlave)
{
    KeyValueMap param;
    uint32_t partitionCount = 2;
    uint32_t parallelNum = 2;
    param["buildStep"] = "increment";
    string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2_updateParallel";
    SingleBuilderTaskV2 controller = createController(configPath);
    std::shared_ptr<common::CheckpointAccessor> checkpointAccessor;
    controller._resourceManager->getResource(checkpointAccessor);
    proto::BuildId buildId = proto::ProtoCreator::createBuildId("simple", 1, "app");
    ClusterCheckpointSynchronizerCreator creator(buildId);
    auto clusterCheckpointSyncs =
        creator.create(checkpointAccessor, configPath, /*metricsReporter=*/nullptr, GET_TEST_DATA_PATH(),
                       ClusterCheckpointSynchronizerCreator::Type::DEFAULT);
    auto checkpointSync = std::make_shared<CheckpointSynchronizer>(buildId);
    checkpointSync->init(clusterCheckpointSyncs, /*isLeaderFollowerMode=*/false);
    std::string errMsg;
    ASSERT_TRUE(checkpointSync->publishPartitionLevelCheckpoint(
        "cluster1", getRange(0, 32767), getVersionMetaStr(getPublishedVersion(1), /*ts=*/1), errMsg));
    ASSERT_TRUE(checkpointSync->publishPartitionLevelCheckpoint(
        "cluster1", getRange(32768, 65535), getVersionMetaStr(getPublishedVersion(1), /*ts=*/1), errMsg));
    ClusterCheckpointSynchronizer::SyncOptions syncOptions;
    syncOptions.isDaily = false;
    syncOptions.isInstant = true;
    syncOptions.isOffline = true;
    syncOptions.addIndexInfos = true;
    ASSERT_TRUE(checkpointSync->syncCluster("cluster1", syncOptions));
    ::google::protobuf::RepeatedPtrField<proto::Range> ranges;
    checkpointid_t newCheckpointId;
    ASSERT_TRUE(checkpointSync->rollback("cluster1", 0, ranges, /*needAddIndexInfo=*/false, &newCheckpointId, errMsg));
    ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult result;
    checkpointSync->createSavepoint("cluster1", newCheckpointId, "", &result, errMsg);
    controller._resourceManager->addResource(checkpointSync);

    param[BS_ROLLBACK_SOURCE_CHECKPOINT] = std::to_string(newCheckpointId);
    controller.init("cluster1", "", "");
    controller.start(param);
    ASSERT_EQ(configPath, getConfigPath(controller));

    // start build full with 2 part, 2 parallel
    checkController(controller, "cluster1", partitionCount, parallelNum, proto::BUILD_STEP_INC);
    auto builders = getBuilders(controller);
    checkBuilders(builders, partitionCount, parallelNum);

    TaskController::Nodes nodes;
    controller.operate(nodes);
    ASSERT_EQ(6u, nodes.size());

    proto::DataDescription dataDescription;
    dataDescription["src"] = "swift";
    dataDescription["type"] = "swift_processed_topic";
    dataDescription["swift_root"] = "zfs://root/";
    dataDescription["swift_start_timestamp"] = "0";
    dataDescription["topic_name"] = "user_name_service_name_processed_1_cluster1";
    dataDescription["src_signature"] = std::to_string(SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
    for (const auto& node : nodes) {
        std::string dataDescStr;
        node.taskTarget.getTargetDescription(DATA_DESCRIPTION_KEY, dataDescStr);
        ASSERT_EQ(ToJsonString(dataDescription), dataDescStr);
    }
    proto::BuildTaskTargetInfo masterTarget;
    masterTarget.finished = false;
    masterTarget.buildMode = proto::BuildMode::PUBLISH;
    masterTarget.alignVersionId = indexlibv2::INVALID_VERSIONID;
    masterTarget.buildStep = "incremental";
    masterTarget.latestPublishedVersion = getPublishedVersion(1);
    masterTarget.indexRoot = "";
    masterTarget.branchId = 1;
    masterTarget.slaveVersionProgress = {};
    checkTarget(nodes, 0, masterTarget);
    checkTarget(nodes, 3, masterTarget);

    proto::BuildTaskTargetInfo slaveTarget;
    slaveTarget.finished = false;
    slaveTarget.buildMode = proto::BuildMode::BUILD;
    slaveTarget.alignVersionId = indexlibv2::INVALID_VERSIONID;
    slaveTarget.buildStep = "incremental";
    slaveTarget.latestPublishedVersion = getPublishedVersion(1);
    slaveTarget.indexRoot = "";
    slaveTarget.branchId = 1;
    slaveTarget.slaveVersionProgress = {};
    checkTarget(nodes, 1, slaveTarget);
    checkTarget(nodes, 2, slaveTarget);
    checkTarget(nodes, 4, slaveTarget);
    checkTarget(nodes, 5, slaveTarget);

    setCurrent(nodes, 1,
               createCurrent(/*fence*/ "", /*versionId*/ getPrivateVersion(3), /*offset*/ 3, /*hasAlignVersion*/ false),
               /*isFinished*/ false);
    setCurrent(nodes, 2,
               createCurrent(/*fence*/ "", /*versionId*/ getPrivateVersion(4), /*offset*/ 3, /*hasAlignVersion*/ false),
               /*isFinished*/ false);
    setCurrent(nodes, 4,
               createCurrent(/*fence*/ "", /*versionId*/ getPrivateVersion(5), /*offset*/ 3, /*hasAlignVersion*/ false),
               /*isFinished*/ false);
    setCurrent(nodes, 5,
               createCurrent(/*fence*/ "", /*versionId*/ getPrivateVersion(6), /*offset*/ 3, /*hasAlignVersion*/ false),
               /*isFinished*/ false);
    controller.operate(nodes);
    const auto& rollbackVersionIdMapping = getRollbackVersionIdMapping(controller);
    ASSERT_EQ(2, rollbackVersionIdMapping.size());
    ASSERT_EQ(getPublishedVersion(1), rollbackVersionIdMapping.begin()->second);
    ASSERT_EQ(getPublishedVersion(1), rollbackVersionIdMapping.rbegin()->second);

    masterTarget.slaveVersionProgress = {{"", getPrivateVersion(3)}, {"", getPrivateVersion(4)}};
    checkTarget(nodes, 0, masterTarget);
    masterTarget.slaveVersionProgress = {{"", getPrivateVersion(5)}, {"", getPrivateVersion(6)}};
    checkTarget(nodes, 3, masterTarget);
    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(3), /*offset*/ 3, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    setCurrent(
        nodes, 3,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(4), /*offset*/ 3, /*hasAlignVersion*/ false),
        /*isFinished*/ false);
    autil::EnvUtil::setEnv(std::string("bs_version_publish_interval_sec"), "0");
    controller.operate(nodes);
    autil::EnvUtil::unsetEnv(std::string("bs_version_publish_interval_sec"));
    ASSERT_EQ(getPublishedVersion(8), getAlignVersionId(controller));
    setCurrent(
        nodes, 0,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(8), /*offset*/ 5, /*hasAlignVersion*/ true),
        /*isFinished*/ false);
    setCurrent(
        nodes, 3,
        createCurrent(/*fence*/ "", /*versionId*/ getPublishedVersion(8), /*offset*/ 5, /*hasAlignVersion*/ true),
        /*isFinished*/ false);
    controller.operate(nodes);
    ASSERT_EQ(0, getRollbackVersionIdMapping(controller).size());
}

}} // namespace build_service::admin
