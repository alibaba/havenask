#include "build_service/common/IndexCheckpointResourceKeeper.h"

#include <ext/alloc_traits.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <string>

#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common/ResourceKeeperGuard.h"
#include "build_service/common_define.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/test/unittest.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace build_service::proto;

namespace build_service { namespace common {

class IndexCheckpointResourceKeeperTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    bool checkResourceStr(const std::string& resource, versionid_t expectVersion);

private:
    common::ResourceContainerPtr _resourceContainer;
    common::CheckpointAccessorPtr _checkpointAccessor;
    config::ResourceReaderPtr _configReader;
    KeyValueMap _kvMap;
};

void IndexCheckpointResourceKeeperTest::setUp()
{
    _resourceContainer.reset(new ResourceContainer);
    _checkpointAccessor.reset(new CheckpointAccessor);
    _resourceContainer->addResource(_checkpointAccessor);
    _kvMap["name"] = "name";
    _kvMap["clusterName"] = "cluster1";
    _kvMap["version"] = "latest";
    string configRootPath = GET_TEST_DATA_PATH() + "build_flow_test/config_src_node";
    _configReader.reset(new config::ResourceReader(configRootPath));
    _configReader->init();
    config::ConfigReaderAccessorPtr accessor(new config::ConfigReaderAccessor("simple"));
    accessor->addConfig(_configReader, true);
    _resourceContainer->addResource(accessor);
}

void IndexCheckpointResourceKeeperTest::tearDown() {}

bool IndexCheckpointResourceKeeperTest::checkResourceStr(const std::string& resource, versionid_t expectVersion)
{
    auto resourceKeeper = ResourceKeeper::deserializeResourceKeeper(resource);
    IndexCheckpointResourceKeeperPtr checkpointKeeper =
        DYNAMIC_POINTER_CAST(IndexCheckpointResourceKeeper, resourceKeeper);
    return expectVersion == checkpointKeeper->getTargetVersionid();
}

TEST_F(IndexCheckpointResourceKeeperTest, testSyncUsingResource)
{
    proto::PartitionId pid;
    proto::WorkerNodes workerNodes;
    workerNodes.push_back(proto::ProcessorNodePtr(new proto::ProcessorNode(pid)));
    workerNodes.push_back(proto::ProcessorNodePtr(new proto::ProcessorNode(pid)));

    IndexCheckpointResourceKeeperPtr resourceKeeper(
        new IndexCheckpointResourceKeeper("name", "index_checkpoint", _resourceContainer));
    ASSERT_TRUE(resourceKeeper->init(_kvMap));
    IndexCheckpointAccessor accessor(_checkpointAccessor);
    proto::CheckpointInfo info;
    info.set_versionid(1);
    accessor.addIndexCheckpoint("cluster1", 1, 3, info);
    ResourceKeeperGuard guard;
    ASSERT_TRUE(guard.init("role", _configReader, resourceKeeper));
    guard.fillResourceKeeper(workerNodes);
    proto::WorkerNodeBase::ResourceInfo* resourceInfo;
    ASSERT_TRUE(workerNodes[0]->getTargetResource("name", resourceInfo));
    string targetResource1 = resourceInfo->resourceStr;
    ASSERT_EQ("1", resourceInfo->resourceId);
    ASSERT_TRUE(checkResourceStr(targetResource1, 1u));

    info.set_versionid(2);
    accessor.addIndexCheckpoint("cluster1", 2, 3, info);
    guard.fillResourceKeeper(workerNodes);
    ASSERT_TRUE(workerNodes[0]->getTargetResource("name", resourceInfo));
    string targetResource2 = resourceInfo->resourceStr;
    ASSERT_EQ("2", resourceInfo->resourceId);
    ASSERT_TRUE(checkResourceStr(resourceInfo->resourceStr, 2u));
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 1));
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 2));

    // check clean
    // one node no using target, not clean
    workerNodes[0]->addUsingResource("name", targetResource2, "2");
    guard.fillResourceKeeper(workerNodes);
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 1));
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 2));

    // one node use version2 one node use version 1, not clean
    workerNodes[1]->addUsingResource("name", targetResource1, "1");
    guard.fillResourceKeeper(workerNodes);
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 1));
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 2));

    // all node use version2
    workerNodes[1]->addUsingResource("name", targetResource2, "2");
    guard.fillResourceKeeper(workerNodes);
    ASSERT_FALSE(accessor.isSavepoint("cluster1", 1));
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 2));

    // check other role apply checkpoint
    ResourceKeeperGuard guard1;
    ASSERT_TRUE(guard1.init("role1", _configReader, resourceKeeper));
    guard1.fillResourceKeeper(workerNodes);
    info.set_versionid(3);
    accessor.addIndexCheckpoint("cluster1", 3, 3, info);
    guard1.fillResourceKeeper(workerNodes);
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 2));
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 3));

    ASSERT_TRUE(workerNodes[0]->getTargetResource("name", resourceInfo));
    ASSERT_EQ("3", resourceInfo->resourceId);
    ASSERT_TRUE(checkResourceStr(resourceInfo->resourceStr, 3u));
    workerNodes[0]->addUsingResource("name", resourceInfo->resourceStr, "3");
    workerNodes[1]->addUsingResource("name", resourceInfo->resourceStr, "3");
    guard1.fillResourceKeeper(workerNodes);
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 2));
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 3));
    guard.fillResourceKeeper(workerNodes);
    ASSERT_FALSE(accessor.isSavepoint("cluster1", 2));
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 3));
}

TEST_F(IndexCheckpointResourceKeeperTest, testDeleteApplyer)
{
    IndexCheckpointResourceKeeper resourceKeeper("name", "index_checkpoint", _resourceContainer);
    ASSERT_TRUE(resourceKeeper.init(_kvMap));
    ASSERT_TRUE(resourceKeeper.prepareResource("role", _configReader));
    ASSERT_EQ(-1u, resourceKeeper.getTargetVersionid());
    IndexCheckpointAccessor accessor(_checkpointAccessor);
    proto::CheckpointInfo info;
    info.set_versionid(1);
    accessor.addIndexCheckpoint("cluster1", 1, 3, info);
    ASSERT_TRUE(resourceKeeper.prepareResource("role", _configReader));
    ASSERT_EQ(1u, resourceKeeper.getTargetVersionid());
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 1));
    info.set_versionid(2);
    accessor.addIndexCheckpoint("cluster1", 2, 3, info);
    ASSERT_TRUE(resourceKeeper.prepareResource("role2", _configReader));
    ASSERT_EQ(2u, resourceKeeper.getTargetVersionid());
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 1));
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 2));
    resourceKeeper.deleteApplyer("role2");
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 1));
    ASSERT_TRUE(accessor.isSavepoint("cluster1", 2));
    resourceKeeper.deleteApplyer("role");
    ASSERT_FALSE(accessor.isSavepoint("cluster1", 2));
    ASSERT_FALSE(accessor.isSavepoint("cluster1", 1));
}

TEST_F(IndexCheckpointResourceKeeperTest, testPrepareResource)
{
    {
        // test no checkpoint accessor
        ResourceContainerPtr resourceContainer(new ResourceContainer);
        IndexCheckpointResourceKeeper resourceKeeper("name", "index_checkpoint", resourceContainer);
        config::ConfigReaderAccessorPtr accessor(new config::ConfigReaderAccessor("simple"));
        accessor->addConfig(_configReader, true);
        resourceContainer->addResource(accessor);
        ASSERT_TRUE(resourceKeeper.init(_kvMap));
        ASSERT_FALSE(resourceKeeper.prepareResource("role", _configReader));
    }
    {
        // test latest checkpoint
        IndexCheckpointResourceKeeper resourceKeeper("name", "index_checkpoint", _resourceContainer);
        ASSERT_TRUE(resourceKeeper.init(_kvMap));
        ASSERT_TRUE(resourceKeeper.prepareResource("role", _configReader));
        ASSERT_EQ(-1u, resourceKeeper.getTargetVersionid());
        IndexCheckpointAccessor accessor(_checkpointAccessor);
        proto::CheckpointInfo info;
        info.set_versionid(1);
        accessor.addIndexCheckpoint("cluster1", 1, 3, info);
        ASSERT_TRUE(resourceKeeper.prepareResource("role", _configReader));
        ASSERT_EQ(1u, resourceKeeper.getTargetVersionid());
        ASSERT_TRUE(accessor.isSavepoint("cluster1", 1));
        info.set_versionid(2);
        accessor.addIndexCheckpoint("cluster1", 2, 3, info);
        ASSERT_TRUE(resourceKeeper.prepareResource("role2", _configReader));
        ASSERT_EQ(2u, resourceKeeper.getTargetVersionid());
        ASSERT_TRUE(accessor.isSavepoint("cluster1", 1));
        ASSERT_TRUE(accessor.isSavepoint("cluster1", 2));
        resourceKeeper.deleteApplyer("role");
        ASSERT_FALSE(accessor.isSavepoint("cluster1", 1));
        ASSERT_TRUE(accessor.isSavepoint("cluster1", 2));
        resourceKeeper.deleteApplyer("role2");
        ASSERT_FALSE(accessor.isSavepoint("cluster1", 2));
    }
    {
        // test no specify checkpoint
        IndexCheckpointResourceKeeper resourceKeeper("name", "index_checkpoint", _resourceContainer);
        KeyValueMap kvMap = _kvMap;
        kvMap["version"] = "0";
        ASSERT_TRUE(resourceKeeper.init(kvMap));
        ASSERT_FALSE(resourceKeeper.prepareResource("role", _configReader));
    }
}

TEST_F(IndexCheckpointResourceKeeperTest, testInit)
{
    {
        IndexCheckpointResourceKeeper resourceKeeper("name", "index_checkpoint", _resourceContainer);
        ASSERT_TRUE(resourceKeeper.init(_kvMap));
    }

    {
        string configRootPath = GET_TEST_DATA_PATH() + "invalid/path";
        common::ResourceContainerPtr resourceContainer(new common::ResourceContainer);
        config::ResourceReaderPtr configReader(new config::ResourceReader(configRootPath));
        configReader->init();
        config::ConfigReaderAccessorPtr accessor(new config::ConfigReaderAccessor("simple"));
        accessor->addConfig(configReader, true);
        resourceContainer->addResource(accessor);
        IndexCheckpointResourceKeeper resourceKeeper("name", "index_checkpoint", resourceContainer);
        KeyValueMap kvMap;
        // test get index root failed
        kvMap["name"] = "name";
        kvMap["clusterName"] = "cluster1";
        kvMap["version"] = "latest";
        ASSERT_FALSE(resourceKeeper.init(kvMap));
    }

    {
        IndexCheckpointResourceKeeper resourceKeeper("name", "index_checkpoint", _resourceContainer);
        KeyValueMap kvMap;
        // test version not right
        kvMap["name"] = "name";
        kvMap["clusterName"] = "cluster1";
        kvMap["version"] = "love";
        ASSERT_FALSE(resourceKeeper.init(kvMap));
    }
}

TEST_F(IndexCheckpointResourceKeeperTest, testUpdateResource)
{
    IndexCheckpointResourceKeeper resourceKeeper("name", "index_checkpoint", _resourceContainer);
    ASSERT_TRUE(resourceKeeper.init(_kvMap));
    IndexCheckpointAccessor accessor(_checkpointAccessor);
    ::google::protobuf::RepeatedPtrField<proto::IndexInfo> indexInfos;
    proto::IndexInfo indexInfo;
    indexInfo.set_clustername("cluster1");
    *indexInfos.Add() = indexInfo;
    accessor.updateIndexInfo(false, "cluster1", indexInfos);
    ASSERT_TRUE(accessor.getIndexInfo(false, "cluster1", indexInfos));
    KeyValueMap invalidMap, validMap;
    invalidMap["visiable"] = "doaisjdioa";
    validMap["visiable"] = "false";
    ASSERT_FALSE(resourceKeeper.updateResource(invalidMap));
    ASSERT_TRUE(accessor.getIndexInfo(false, "cluster1", indexInfos));
    ASSERT_TRUE(resourceKeeper.updateResource(validMap));
    ASSERT_FALSE(accessor.getIndexInfo(false, "cluster1", indexInfos));
    validMap["visiable"] = "true";
    ASSERT_TRUE(resourceKeeper.updateResource(validMap));
    ASSERT_TRUE(accessor.getIndexInfo(false, "cluster1", indexInfos));
}

}} // namespace build_service::common
