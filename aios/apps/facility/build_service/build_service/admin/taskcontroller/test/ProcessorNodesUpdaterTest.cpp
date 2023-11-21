#include "build_service/admin/taskcontroller/ProcessorNodesUpdater.h"

#include <cstdint>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/admin/taskcontroller/ProcessorInput.h"
#include "build_service/admin/taskcontroller/ProcessorTargetInfos.h"
#include "build_service/admin/taskcontroller/ProcessorWriterVersion.h"
#include "build_service/common/ProcessorOutput.h"
#include "build_service/common_define.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "build_service/test/unittest.h"
#include "hippo/proto/Common.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace admin {

class ProcessorNodesUpdaterTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    proto::BuildId _buildId;
};

void ProcessorNodesUpdaterTest::setUp()
{
    _buildId.set_generationid(1);
    _buildId.set_datatable("simple");
}

void ProcessorNodesUpdaterTest::tearDown() {}

TEST_F(ProcessorNodesUpdaterTest, testSimple)
{
    vector<string> clusterNames;
    config::ResourceReaderPtr configReader(new config::ResourceReader(""));
    shared_ptr<taskcontroller::NodeStatusManager> nodeStatusManager(new taskcontroller::NodeStatusManager(_buildId, 0));
    ProcessorInput input;
    ProcessorWriterVersion writerVersion;
    input.src = 0;
    input.offset = -1;
    proto::DataDescription ds;
    ds["topicname"] = "topicname";
    input.dataDescriptions.push_back(ds);
    common::ProcessorOutput output;
    ProcessorNodesUpdater updater(configReader, nodeStatusManager, clusterNames, {input, output}, 0, "", false,
                                  /*needSafeWrite*/ true);
    auto nodes =
        proto::WorkerNodeCreator<proto::ROLE_PROCESSOR>::createNodes(2, 1, proto::BUILD_STEP_INC, _buildId, "", "", "");
    ASSERT_EQ(2, nodes.size());
    ProcessorBasicInfo basicInfo(/*src*/ 0, /*offset*/ -1, /*partitionCount*/ 2, /*parallelNum*/ 1);
    ProcessorTargetInfos lastTargetInfo;

    [[maybe_unused]] auto ret = writerVersion.update(nodes, 2, 1);
    updater.update(nodes, writerVersion, basicInfo, lastTargetInfo);
    auto checkOffsetFunc = [&](auto node, int64_t expectedOffset) {
        static int32_t checkIdx = 0;
        checkIdx++;
        auto target = node->getTargetStatus();
        auto locator = target.startlocator();
        ASSERT_EQ(expectedOffset, locator.checkpoint()) << " check failed at idx" << checkIdx << endl;
    };
    auto setOffsetFunc = [&](auto node, int64_t offset) {
        proto::ProcessorCurrent current;
        current.mutable_currentlocator()->set_sourcesignature(0);
        current.mutable_currentlocator()->set_checkpoint(offset);
        node->setCurrentStatus(current);
    };
    checkOffsetFunc(nodes[0], -1);
    checkOffsetFunc(nodes[1], -1);

    setOffsetFunc(nodes[0], 5);

    ret = writerVersion.update(nodes, 2, 1);
    updater.update(nodes, writerVersion, basicInfo, lastTargetInfo);
    checkOffsetFunc(nodes[0], 5);
    checkOffsetFunc(nodes[1], -1);

    proto::ProcessorCurrent emptyCurrent;
    nodes[0]->setCurrentStatus(emptyCurrent);
    updater.update(nodes, writerVersion, basicInfo, lastTargetInfo);
    checkOffsetFunc(nodes[0], 5);

    // src change
    basicInfo.src = 1;
    basicInfo.offset = 10;
    ret = writerVersion.update(nodes, 2, 1);
    updater.update(nodes, writerVersion, basicInfo, lastTargetInfo);
    checkOffsetFunc(nodes[0], 10);
    checkOffsetFunc(nodes[1], 10);

    // partition change
    basicInfo.offset = 15;
    basicInfo.partitionCount = 3;
    basicInfo.parallelNum = 3;
    nodes =
        proto::WorkerNodeCreator<proto::ROLE_PROCESSOR>::createNodes(3, 3, proto::BUILD_STEP_INC, _buildId, "", "", "");
    ret = writerVersion.update(nodes, 3, 3);
    updater.update(nodes, writerVersion, basicInfo, lastTargetInfo);
    ASSERT_EQ(9, nodes.size());
    for (auto i = 0; i < 9; i++) {
        checkOffsetFunc(nodes[i], 15);
    }
}

TEST_F(ProcessorNodesUpdaterTest, testUpdateWithWriterVersion)
{
    vector<string> clusterNames;
    config::ResourceReaderPtr configReader(new config::ResourceReader(""));
    shared_ptr<taskcontroller::NodeStatusManager> nodeStatusManager(new taskcontroller::NodeStatusManager(_buildId, 0));
    ProcessorInput input;
    input.src = 0;
    input.offset = 0;
    proto::DataDescription ds;
    ds["topicname"] = "topicname";
    input.dataDescriptions.push_back(ds);
    common::ProcessorOutput output;
    ProcessorNodesUpdater updater(configReader, nodeStatusManager, clusterNames, {input, output}, 0, "", false,
                                  /*needSafeWrite*/ true);
    auto nodes =
        proto::WorkerNodeCreator<proto::ROLE_PROCESSOR>::createNodes(2, 1, proto::BUILD_STEP_INC, _buildId, "", "", "");
    ASSERT_EQ(2, nodes.size());
    ProcessorBasicInfo basicInfo(/*src*/ 0, /*offset*/ 0, /*partitionCount*/ 2, /*parallelNum*/ 1);
    ProcessorTargetInfos lastTargetInfo;
    std::string host1 = "100.100.100.100";
    std::string host2 = "100.100.100.101";
    auto identifier1 = proto::WorkerNodeBase::getIdentifier(host1, 1);
    auto identifier2 = proto::WorkerNodeBase::getIdentifier(host2, 1);
    ProcessorWriterVersion writerVersion = {123, {11, 12}, {identifier1, identifier2}};

    auto checkTargetFunc = [&](auto node, int64_t expectedOffset, pair<uint32_t, uint32_t> expectedWriterVersion,
                               std::string expectedIdentifier) {
        ASSERT_TRUE(node->isReady());
        static int32_t checkIdx = 0;
        checkIdx++;
        auto target = node->getTargetStatus();
        auto locator = target.startlocator();
        KeyValueMap kvMap;
        autil::legacy::FromJsonString(kvMap, target.targetdescription());
        auto majorVersion = kvMap[config::SWIFT_MAJOR_VERSION];
        auto minorVersion = kvMap[config::SWIFT_MINOR_VERSION];
        ASSERT_EQ(expectedOffset, locator.checkpoint()) << " check failed at idx" << checkIdx << endl;

        auto processorNode = std::dynamic_pointer_cast<proto::WorkerNode<proto::ROLE_PROCESSOR>>(node);
        ASSERT_TRUE(processorNode);
        std::string targetStr, targetIdentifier;
        processorNode->getTargetStatusStr(targetStr, targetIdentifier);
        ASSERT_EQ(expectedIdentifier, targetIdentifier) << " check identifier failed at idx" << checkIdx << endl;
        ASSERT_EQ(std::to_string(expectedWriterVersion.first), majorVersion)
            << " check major version failed at idx" << checkIdx << endl;
        ASSERT_EQ(std::to_string(expectedWriterVersion.second), minorVersion)
            << " check minor version failed at idx" << checkIdx << endl;
    };

    auto addSlotInfoFunc = [&](auto node, const std::string& host) {
        hippo::proto::AssignedSlot singleSlotInfo;
        singleSlotInfo.set_reclaim(false);
        singleSlotInfo.mutable_id()->set_slaveaddress(host);
        singleSlotInfo.mutable_id()->set_id(1);
        ::google::protobuf::RepeatedPtrField<hippo::proto::AssignedSlot> slotInfos;
        *slotInfos.Add() = singleSlotInfo;
        node->setSlotInfo(slotInfos);
    };
    updater.update(nodes, writerVersion, basicInfo, lastTargetInfo);
    ASSERT_FALSE(nodes[0]->isReady());
    ASSERT_FALSE(nodes[1]->isReady());

    addSlotInfoFunc(nodes[0], host1);
    updater.update(nodes, writerVersion, basicInfo, lastTargetInfo);

    checkTargetFunc(nodes[0], 0, make_pair(123, 11), identifier1);
    ASSERT_TRUE(nodes[0]->isReady());
    ASSERT_FALSE(nodes[1]->isReady());

    addSlotInfoFunc(nodes[1], host2);
    updater.update(nodes, writerVersion, basicInfo, lastTargetInfo);
    checkTargetFunc(nodes[0], 0, make_pair(123, 11), identifier1);
    checkTargetFunc(nodes[1], 0, make_pair(123, 12), identifier2);
    ASSERT_TRUE(nodes[0]->isReady());
    ASSERT_TRUE(nodes[1]->isReady());
}

}} // namespace build_service::admin
