#include "build_service/common/CheckpointResourceKeeper.h"

#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/ResourceCheckpointFormatter.h"
#include "build_service/common/ResourceKeeperCreator.h"
#include "build_service/common/ResourceKeeperGuard.h"
#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace common {

class CheckpointResourceKeeperTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    ResourceContainerPtr _resourceContainer;
    CheckpointAccessorPtr _ckptAccessor;
};

void CheckpointResourceKeeperTest::setUp()
{
    _resourceContainer.reset(new ResourceContainer);
    _ckptAccessor.reset(new CheckpointAccessor);
    _resourceContainer->addResource(_ckptAccessor);
}

void CheckpointResourceKeeperTest::tearDown() {}

TEST_F(CheckpointResourceKeeperTest, testCreate)
{
    auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
    ASSERT_TRUE(keeper.get());
}

TEST_F(CheckpointResourceKeeperTest, testInit)
{
    {
        // missing checkpoint type
        auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
        KeyValueMap kvMap;
        ASSERT_FALSE(keeper->init(kvMap));
    }
    {
        // invalid resource version
        auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
        KeyValueMap kvMap;
        kvMap["checkpoint_type"] = "syncIndex";
        kvMap["resource_version"] = "abcde";
        ASSERT_FALSE(keeper->init(kvMap));
    }
    {
        // invalid need latest version
        auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
        KeyValueMap kvMap;
        kvMap["checkpoint_type"] = "syncIndex";
        kvMap["need_latest_version"] = "abcde";
        ASSERT_FALSE(keeper->init(kvMap));
    }
    {
        // missing checkpoint type
        auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
        KeyValueMap kvMap;
        kvMap["checkpoint_type"] = "syncIndex";
        kvMap["need_latest_version"] = "false";
        kvMap["resource_version"] = "500";
        ASSERT_TRUE(keeper->init(kvMap));
    }
}

TEST_F(CheckpointResourceKeeperTest, testAdd)
{
    auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
    KeyValueMap kvMap;
    ASSERT_TRUE(keeper.get());
    ASSERT_FALSE(keeper->addResource(kvMap));
    kvMap["checkpoint_type"] = "syncIndex";
    ASSERT_TRUE(keeper->addResource(kvMap));
    kvMap["indexRoot"] = "dfs://xxx";
    ASSERT_TRUE(keeper->addResource(kvMap));
    std::string comment;
    auto checkFunc = [&](versionid_t version) {
        return _ckptAccessor->isSavepoint(ResourceCheckpointFormatter::getResourceCheckpointId("syncIndex"),
                                          ResourceCheckpointFormatter::encodeResourceCheckpointName(version), &comment);
    };
    {
        // get version 0
        auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
        KeyValueMap kvMap;
        kvMap["checkpoint_type"] = "syncIndex";
        kvMap["resource_version"] = "0";
        ASSERT_TRUE(keeper->init(kvMap));
        ASSERT_TRUE(keeper->prepareResource("applyer", nullptr));
        string value;
        ASSERT_FALSE(keeper->getParam("indexRoot", &value));
        ASSERT_TRUE(checkFunc(0));
        keeper->deleteApplyer("applyer");
        ASSERT_FALSE(checkFunc(0));
    }
    {
        // get version 1
        auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
        KeyValueMap kvMap;
        kvMap["checkpoint_type"] = "syncIndex";
        kvMap["resource_version"] = "1";
        ASSERT_TRUE(keeper->init(kvMap));
        ASSERT_TRUE(keeper->prepareResource("applyer", nullptr));
        string value;
        ASSERT_TRUE(keeper->getParam("indexRoot", &value));
        ASSERT_EQ("dfs://xxx", value);
        ASSERT_FALSE(checkFunc(0));
        ASSERT_TRUE(checkFunc(1));
        keeper->deleteApplyer("applyer");
        ASSERT_FALSE(checkFunc(0));
        ASSERT_FALSE(checkFunc(1));
    }
    {
        // no version 2
        auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
        KeyValueMap kvMap;
        kvMap["checkpoint_type"] = "syncIndex";
        kvMap["resource_version"] = "2";
        ASSERT_TRUE(keeper->init(kvMap));
        ASSERT_FALSE(keeper->prepareResource("applyer", nullptr));
        ASSERT_FALSE(checkFunc(0));
        ASSERT_FALSE(checkFunc(1));
    }
    {
        // get latest version
        auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
        KeyValueMap kvMap;
        kvMap["checkpoint_type"] = "syncIndex";
        kvMap["need_latest_version"] = "true";
        ASSERT_TRUE(keeper->init(kvMap));
        ASSERT_TRUE(keeper->prepareResource("applyer", nullptr));
        ASSERT_FALSE(checkFunc(0));
        ASSERT_TRUE(checkFunc(1));
        string value;
        ASSERT_TRUE(keeper->getParam("indexRoot", &value));
        ASSERT_EQ("dfs://xxx", value);
        keeper->deleteApplyer("applyer");
        ASSERT_FALSE(checkFunc(0));
        ASSERT_FALSE(checkFunc(1));
    }
}

TEST_F(CheckpointResourceKeeperTest, testDelete)
{
    auto checkFunc = [&](versionid_t version) {
        string ckptValue;
        return _ckptAccessor->getCheckpoint(ResourceCheckpointFormatter::getResourceCheckpointId("syncIndex"),
                                            ResourceCheckpointFormatter::encodeResourceCheckpointName(version),
                                            ckptValue);
    };
    auto keeper = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
    KeyValueMap kvMap;
    ASSERT_TRUE(keeper.get());
    ASSERT_FALSE(keeper->addResource(kvMap));
    kvMap["checkpoint_type"] = "syncIndex";
    ASSERT_TRUE(keeper->addResource(kvMap));
    kvMap["indexRoot"] = "dfs://xxx";
    ASSERT_TRUE(keeper->addResource(kvMap));

    // book version 0
    auto keeper1 = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
    KeyValueMap kvMap1;
    kvMap1["checkpoint_type"] = "syncIndex";
    kvMap1["resource_version"] = "0";
    ASSERT_TRUE(keeper1->init(kvMap1));
    ASSERT_TRUE(keeper1->prepareResource("applyer1", nullptr));

    // book version 1
    auto keeper2 = ResourceKeeperCreator::create("name", "checkpoint", _resourceContainer);
    KeyValueMap kvMap2;
    kvMap2["checkpoint_type"] = "syncIndex";
    kvMap2["resource_version"] = "1";
    ASSERT_TRUE(keeper2->init(kvMap2));
    ASSERT_TRUE(keeper2->prepareResource("applyer2", nullptr));

    KeyValueMap deleteMap;
    ASSERT_FALSE(keeper->deleteResource(deleteMap));
    deleteMap["checkpoint_type"] = "syncIndex";
    ASSERT_FALSE(keeper->deleteResource(deleteMap));
    deleteMap["resource_version"] = "0";
    ASSERT_TRUE(keeper->deleteResource(deleteMap));
    ASSERT_TRUE(checkFunc(0));

    keeper1->deleteApplyer("applyer1");
    ASSERT_TRUE(keeper->deleteResource(deleteMap));
    ASSERT_FALSE(checkFunc(0));
}

TEST_F(CheckpointResourceKeeperTest, testSync)
{
    auto checkFunc = [&](versionid_t targetVersion, const string& resourceStr) {
        auto resourceKeeper = ResourceKeeper::deserializeResourceKeeper(resourceStr);
        string realVersion;
        ASSERT_TRUE(resourceKeeper->getParam("index_version", &realVersion));
        ASSERT_EQ(autil::StringUtil::toString(targetVersion), realVersion);
    };

    std::string comment;
    auto checkSavepoint = [&](versionid_t version) {
        return _ckptAccessor->isSavepoint(ResourceCheckpointFormatter::getResourceCheckpointId("syncIndex"),
                                          ResourceCheckpointFormatter::encodeResourceCheckpointName(version), &comment);
    };

    proto::PartitionId pid;
    proto::WorkerNodes workerNodes;
    workerNodes.push_back(proto::ProcessorNodePtr(new proto::ProcessorNode(pid)));
    workerNodes.push_back(proto::ProcessorNodePtr(new proto::ProcessorNode(pid)));

    CheckpointResourceKeeperPtr resourceKeeper(new CheckpointResourceKeeper("name", "checkpoint", _resourceContainer));
    CheckpointResourceKeeperPtr tmpKeeper(new CheckpointResourceKeeper("name1", "checkpoint", _resourceContainer));
    KeyValueMap tmpKvMap;
    tmpKvMap["checkpoint_type"] = "syncIndex";
    ASSERT_TRUE(tmpKeeper->init(tmpKvMap));
    auto checkSavepointByKeeper = [&](set<versionid_t> savepointed, set<versionid_t> unsavepointed) {
        auto status = tmpKeeper->getResourceStatus();
        ASSERT_EQ(savepointed.size() + unsavepointed.size(), status.size());
        for (auto it : status) {
            versionid_t version = it.version;
            if (savepointed.find(version) != savepointed.end()) {
                ASSERT_EQ(CheckpointResourceKeeper::CRS_SAVEPOINTED, it.status);
            } else {
                ASSERT_NE(unsavepointed.end(), unsavepointed.find(version));
                ASSERT_EQ(CheckpointResourceKeeper::CRS_UNSAVEPOINTED, it.status);
            }
        }
    };
    KeyValueMap initMap;
    initMap["checkpoint_type"] = "syncIndex";
    initMap["need_latest_version"] = "true";
    ASSERT_TRUE(resourceKeeper->init(initMap));
    KeyValueMap kvMap;
    kvMap["checkpoint_type"] = "syncIndex";
    kvMap["index_version"] = "100";
    ASSERT_TRUE(tmpKeeper->addResource(kvMap));
    ResourceKeeperGuard guard;
    ASSERT_TRUE(guard.init("role", nullptr, resourceKeeper));
    guard.fillResourceKeeper(workerNodes);
    proto::WorkerNodeBase::ResourceInfo* resourceInfo;
    ASSERT_TRUE(workerNodes[0]->getTargetResource("name", resourceInfo));
    string targetResource1 = resourceInfo->resourceStr;
    ASSERT_EQ("0", resourceInfo->resourceId);
    checkFunc(100, targetResource1);

    kvMap["index_version"] = "200";
    ASSERT_TRUE(tmpKeeper->addResource(kvMap));
    guard.fillResourceKeeper(workerNodes);
    ASSERT_TRUE(workerNodes[0]->getTargetResource("name", resourceInfo));
    string targetResource2 = resourceInfo->resourceStr;
    ASSERT_EQ("1", resourceInfo->resourceId);
    checkFunc(200, resourceInfo->resourceStr);
    checkSavepointByKeeper({0, 1}, {});
    ASSERT_TRUE(checkSavepoint(0));
    ASSERT_TRUE(checkSavepoint(1));
    // check clean
    // one node no using target, not clean
    workerNodes[0]->addUsingResource("name", targetResource2, "1");
    guard.fillResourceKeeper(workerNodes);
    ASSERT_TRUE(checkSavepoint(0));
    ASSERT_TRUE(checkSavepoint(1));
    checkSavepointByKeeper({0, 1}, {});

    // one node use version2 one node use version 1, not clean
    workerNodes[1]->addUsingResource("name", targetResource1, "0");
    guard.fillResourceKeeper(workerNodes);
    ASSERT_TRUE(checkSavepoint(0));
    ASSERT_TRUE(checkSavepoint(1));
    checkSavepointByKeeper({0, 1}, {});

    // all node use version2
    workerNodes[1]->addUsingResource("name", targetResource2, "1");
    guard.fillResourceKeeper(workerNodes);
    ASSERT_FALSE(checkSavepoint(0));
    ASSERT_TRUE(checkSavepoint(1));
    checkSavepointByKeeper({1}, {0});

    // check other role apply checkpoint
    ResourceKeeperGuard guard1;
    ASSERT_TRUE(guard1.init("role1", nullptr, resourceKeeper));
    guard1.fillResourceKeeper(workerNodes);
    kvMap["index_version"] = "300";
    ASSERT_TRUE(tmpKeeper->addResource(kvMap));
    guard1.fillResourceKeeper(workerNodes);
    ASSERT_TRUE(checkSavepoint(1));
    ASSERT_TRUE(checkSavepoint(2));
    checkSavepointByKeeper({1, 2}, {0});

    ASSERT_TRUE(workerNodes[0]->getTargetResource("name", resourceInfo));
    ASSERT_EQ("2", resourceInfo->resourceId);
    checkFunc(300, resourceInfo->resourceStr);
    workerNodes[0]->addUsingResource("name", resourceInfo->resourceStr, "2");
    workerNodes[1]->addUsingResource("name", resourceInfo->resourceStr, "2");
    guard1.fillResourceKeeper(workerNodes);
    ASSERT_TRUE(checkSavepoint(1));
    ASSERT_TRUE(checkSavepoint(2));
    checkSavepointByKeeper({1, 2}, {0});
    guard.fillResourceKeeper(workerNodes);
    ASSERT_FALSE(checkSavepoint(1));
    ASSERT_TRUE(checkSavepoint(2));
    checkSavepointByKeeper({2}, {0, 1});
}

}} // namespace build_service::common
