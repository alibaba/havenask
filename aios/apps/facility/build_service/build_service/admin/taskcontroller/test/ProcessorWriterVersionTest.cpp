#include "build_service/admin/taskcontroller/ProcessorWriterVersion.h"

#include <iosfwd>
#include <stdint.h>
#include <vector>

#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace admin {

class ProcessorWriterVersionTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    proto::BuildId _buildId;
};

void ProcessorWriterVersionTest::setUp()
{
    _buildId.set_generationid(1);
    _buildId.set_datatable("simple");
}

void ProcessorWriterVersionTest::tearDown() {}

TEST_F(ProcessorWriterVersionTest, testSimple)
{
    ProcessorWriterVersion writerVersion;
    auto createNodes = [&](uint32_t partitionCount, uint32_t parallelNum) {
        return proto::WorkerNodeCreator<proto::ROLE_PROCESSOR>::createNodes(partitionCount, parallelNum,
                                                                            proto::BUILD_STEP_INC, _buildId);
    };
    auto nodes1 = createNodes(1, 2);
    ASSERT_TRUE(writerVersion.update(nodes1, 1, 2));
    auto nodes2 = createNodes(2, 1);
    ASSERT_TRUE(writerVersion.update(nodes2, 2, 1));
    ASSERT_FALSE(writerVersion.update(nodes2, 2, 1));
}

TEST_F(ProcessorWriterVersionTest, testForceUpdateMajorVersion)
{
    ProcessorWriterVersion writerVersion;
    auto createNodes = [&](uint32_t partitionCount, uint32_t parallelNum) {
        return proto::WorkerNodeCreator<proto::ROLE_PROCESSOR>::createNodes(partitionCount, parallelNum,
                                                                            proto::BUILD_STEP_INC, _buildId);
    };
    auto nodes1 = createNodes(1, 2);
    ASSERT_TRUE(writerVersion.update(nodes1, 1, 2));
    auto beforeMajorVersion = writerVersion.getMajorVersion();

    auto updateInfo = writerVersion.forceUpdateMajorVersion(2);

    ASSERT_EQ(beforeMajorVersion + 1, updateInfo.updatedMajorVersion);
    ASSERT_EQ(2, updateInfo.updatedMinorVersions.size());
}

}} // namespace build_service::admin
