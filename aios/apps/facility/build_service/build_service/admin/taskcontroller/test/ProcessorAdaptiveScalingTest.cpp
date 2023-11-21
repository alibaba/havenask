#include "build_service/admin/taskcontroller/ProcessorAdaptiveScaling.h"

#include <iosfwd>
#include <memory>
#include <optional>
#include <stdint.h>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/config/ProcessorAdaptiveScalingConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service::admin {

class ProcessorAdaptiveScalingTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
    void setNodeInfo(int64_t currentTime, int64_t delayLocator, int64_t cpuRatio, bool errorExceedThreadhold,
                     int32_t delayWorker, proto::ProcessorNodes& nodes);
};

void ProcessorAdaptiveScalingTest::setUp() {}

void ProcessorAdaptiveScalingTest::tearDown() {}

void ProcessorAdaptiveScalingTest::setNodeInfo(int64_t currentTime, int64_t delayLocator, int64_t cpuRatio,
                                               bool errorExceedThreadhold, int32_t delayWorker,
                                               proto::ProcessorNodes& nodes)
{
    int i = 0;
    int64_t oneMinute = 1 * 60 * 1000 * 1000;
    for (auto& node : nodes) {
        proto::ProcessorCurrent current;
        if (i++ < delayWorker) {
            current.mutable_currentlocator()->set_checkpoint(delayLocator);
        } else {
            current.mutable_currentlocator()->set_checkpoint(currentTime);
        }
        current.mutable_progressstatus()->set_starttimestamp(currentTime - 20 * oneMinute);
        current.mutable_progressstatus()->set_reporttimestamp(currentTime);
        current.mutable_progressstatus()->set_progress(currentTime);
        current.mutable_progressstatus()->set_startpoint(currentTime);
        current.mutable_machinestatus()->set_cpuratioinwindow(cpuRatio);
        current.mutable_machinestatus()->set_emacpuspeed(100000000);
        current.set_workflowerrorexceedthreadhold(errorExceedThreadhold);
        current.set_status(proto::WS_STARTED);
        node->setCurrentStatus(current);
    }
}

TEST_F(ProcessorAdaptiveScalingTest, testSimple)
{
    config::ProcessorAdaptiveScalingConfig config;
    config.maxCount = 20;
    ProcessorAdaptiveScaling adaptiveScaling(config, "swift", false);
    proto::BuildId buildId;
    uint32_t partitionCount = 4;
    uint32_t parallelNum = 1;
    auto nodes = proto::WorkerNodeCreator<proto::ROLE_PROCESSOR>::createNodes(
        partitionCount, parallelNum, proto::BUILD_STEP_INC, buildId, "", "", "");

    int64_t currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    auto manager = std::make_shared<taskcontroller::NodeStatusManager>(buildId, currentTime);
    int64_t oneMinute = 1 * 60 * 1000 * 1000;
    int64_t delayLocator = currentTime - 15 * oneMinute;

    setNodeInfo(currentTime, delayLocator, -1, false, 3, nodes);
    manager->Update(nodes);
    auto ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_FALSE(ret.has_value());

    // 75% 延迟
    setNodeInfo(currentTime + oneMinute, delayLocator + 1, -1, false, 3, nodes);
    manager->Update(nodes);
    ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_TRUE(ret.has_value());
    ASSERT_EQ(20, ret.value().first);
    ASSERT_EQ(1, ret.value().second);

    partitionCount = ret.value().first;
    parallelNum = ret.value().second;
    // 再次延迟，保持max count
    nodes = proto::WorkerNodeCreator<proto::ROLE_PROCESSOR>::createNodes(partitionCount, parallelNum,
                                                                         proto::BUILD_STEP_INC, buildId, "", "", "");
    setNodeInfo(currentTime + 2 * oneMinute, delayLocator, -1, false, 8, nodes);
    manager->Update(nodes);
    ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);

    setNodeInfo(currentTime + 3 * oneMinute, delayLocator + 1, -1, false, 8, nodes);
    manager->Update(nodes);
    ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_FALSE(ret.has_value());

    // cpu ratio 是0，缩小到1
    setNodeInfo(currentTime + 4 * oneMinute, currentTime, 0, false, 4, nodes);
    manager->Update(nodes);
    ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_TRUE(ret.has_value());
    ASSERT_EQ(1, ret.value().first);
    ASSERT_EQ(1, ret.value().second);
}

TEST_F(ProcessorAdaptiveScalingTest, testLocatorScaling)
{
    config::ProcessorAdaptiveScalingConfig config;
    config.maxCount = 10;
    ProcessorAdaptiveScaling adaptiveScaling(config, "swift", false);
    proto::BuildId buildId;
    uint32_t partitionCount = 4;
    uint32_t parallelNum = 1;
    auto nodes = proto::WorkerNodeCreator<proto::ROLE_PROCESSOR>::createNodes(
        partitionCount, parallelNum, proto::BUILD_STEP_INC, buildId, "", "", "");

    int64_t currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    auto manager = std::make_shared<taskcontroller::NodeStatusManager>(buildId, currentTime);
    int64_t oneMinute = 1 * 60 * 1000 * 1000;
    int64_t delayLocator = currentTime - 15 * oneMinute;

    setNodeInfo(currentTime, delayLocator, -1, false, 2, nodes);
    manager->Update(nodes);
    auto ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_FALSE(ret.has_value());
    // 50% 延迟, 但是locator下降速度快，不进行扩容
    setNodeInfo(currentTime + oneMinute, delayLocator + 4 * oneMinute, -1, false, 2, nodes);
    manager->Update(nodes);
    ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_FALSE(ret.has_value());
    // 恢复了
    setNodeInfo(currentTime + 2 * oneMinute, currentTime, -1, false, 2, nodes);
    manager->Update(nodes);
    ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_FALSE(ret.has_value());
    setNodeInfo(currentTime + 3 * oneMinute, delayLocator, -1, false, 2, nodes);
    manager->Update(nodes);
    ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_FALSE(ret.has_value());
    //再次延迟，但是gap过短，暂不处理
    setNodeInfo(currentTime + 3 * oneMinute + 1, delayLocator + 1, -1, false, 2, nodes);
    manager->Update(nodes);
    ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_FALSE(ret.has_value());
    //延迟处理,扩4倍，以max count为准
    setNodeInfo(currentTime + 4 * oneMinute, delayLocator + 2, -1, false, 2, nodes);
    manager->Update(nodes);
    ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_TRUE(ret.has_value());
    ASSERT_EQ(10, ret.value().first);
    ASSERT_EQ(1, ret.value().second);
}

TEST_F(ProcessorAdaptiveScalingTest, testAdaptiveScalingFixedMax)
{
    config::ProcessorAdaptiveScalingConfig config;
    config.maxCount = 10;
    proto::BuildId buildId;
    int64_t currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    auto manager = std::make_shared<taskcontroller::NodeStatusManager>(buildId, currentTime);
    ProcessorAdaptiveScaling adaptiveScaling(config, "swift", true);
    auto ret = adaptiveScaling.handle(manager, 1, 1);
    ASSERT_TRUE(ret.has_value());
    ASSERT_EQ(10, ret.value().first);
    ASSERT_EQ(1, ret.value().second);

    ret = adaptiveScaling.handle(manager, ret.value().first, ret.value().second);
    ASSERT_FALSE(ret.has_value());
}

TEST_F(ProcessorAdaptiveScalingTest, testSrcNotSame)
{
    config::ProcessorAdaptiveScalingConfig config;
    config.maxCount = 10;
    ProcessorAdaptiveScaling adaptiveScaling(config, "file", false);
    proto::BuildId buildId;
    uint32_t partitionCount = 4;
    uint32_t parallelNum = 1;
    auto nodes = proto::WorkerNodeCreator<proto::ROLE_PROCESSOR>::createNodes(
        partitionCount, parallelNum, proto::BUILD_STEP_INC, buildId, "", "", "");

    int64_t currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    auto manager = std::make_shared<taskcontroller::NodeStatusManager>(buildId, currentTime);

    // cpu低，应该缩容，但是src不同，不调整
    setNodeInfo(currentTime, currentTime, 2, false, 2, nodes);
    manager->Update(nodes);
    auto ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_FALSE(ret.has_value());
}

TEST_F(ProcessorAdaptiveScalingTest, testAdaptiveConfigDisable)
{
    config::ProcessorAdaptiveScalingConfig config;
    config.maxCount = 10;
    config.latencyStrategy.enabled = false;
    ProcessorAdaptiveScaling adaptiveScaling(config, "swift", false);
    proto::BuildId buildId;
    uint32_t partitionCount = 4;
    uint32_t parallelNum = 1;
    auto nodes = proto::WorkerNodeCreator<proto::ROLE_PROCESSOR>::createNodes(
        partitionCount, parallelNum, proto::BUILD_STEP_INC, buildId, "", "", "");

    int64_t currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    auto manager = std::make_shared<taskcontroller::NodeStatusManager>(buildId, currentTime);
    int64_t oneMinute = 1 * 60 * 1000 * 1000;
    int64_t delayLocator = currentTime - 15 * oneMinute;

    setNodeInfo(currentTime, delayLocator, -1, false, 2, nodes);
    manager->Update(nodes);
    auto ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_FALSE(ret.has_value());

    // 50% 延迟, 但因为关闭了locator调整没有值
    setNodeInfo(currentTime, delayLocator + 1, -1, false, 2, nodes);
    manager->Update(nodes);
    ret = adaptiveScaling.handle(manager, partitionCount, parallelNum);
    ASSERT_FALSE(ret.has_value());
}

} // namespace build_service::admin
