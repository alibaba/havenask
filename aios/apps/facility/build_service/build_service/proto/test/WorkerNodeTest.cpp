#include "build_service/proto/WorkerNode.h"

#include <iosfwd>
#include <stdint.h>
#include <string>

#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace proto {

class WorkerNodeTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void WorkerNodeTest::setUp() {}

void WorkerNodeTest::tearDown() {}

TEST_F(WorkerNodeTest, testSetTargetWithRequiredHost)
{
    PartitionId pid;
    WorkerNode<ROLE_PROCESSOR> a(pid);
    ProcessorTarget target;
    target.set_configpath("test_config_path");
    a.setTargetStatus(target, "100.100.100.100");
    std::string requiredHost;
    std::string targetStr;
    a.getTargetStatusStr(targetStr, requiredHost);
    ASSERT_EQ("100.100.100.100", requiredHost);
}

TEST_F(WorkerNodeTest, testIdentifier)
{
    std::string ip = "100.100.100.100";
    int32_t slotId = 123;
    auto identifier = WorkerNodeBase::getIdentifier(ip, slotId);
    auto [retIp, retSlotId] = WorkerNodeBase::getIpAndSlotId(identifier);
    ASSERT_EQ(ip, retIp);
    ASSERT_EQ("123", retSlotId);
}

}} // namespace build_service::proto
