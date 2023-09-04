#include "build_service/proto/RoleNameGenerator.h"

#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace proto {

class RoleNameGeneratorTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() {};
    void tearDown() {};
};

TEST_F(RoleNameGeneratorTest, testGenerateGoupName)
{
    PartitionId pid;
    pid.set_role(ROLE_TASK);
    pid.mutable_range()->set_from(0);
    pid.mutable_range()->set_to(65535);
    pid.mutable_buildid()->set_datatable("datatable");
    pid.mutable_buildid()->set_generationid(111);
    pid.mutable_buildid()->set_appname("appname");
    *pid.add_clusternames() = "clustername";
    pid.set_taskid("taskId=fullBuilder-name=slave-taskName=builderV2");

    std::string groupName = RoleNameGenerator::generateGroupName(pid);
    std::string expectedGroupName = "builder.clustername.full";
    ASSERT_EQ(expectedGroupName, groupName);
    pid.set_taskid("taskId=incBuilder-name=slave-taskName=builderV2");
    groupName = RoleNameGenerator::generateGroupName(pid);
    expectedGroupName = "builder.clustername.inc";
    ASSERT_EQ(expectedGroupName, groupName);

    pid.set_taskid("taskId=111-name=fullMerge");
    groupName = RoleNameGenerator::generateGroupName(pid);
    expectedGroupName = "merger.clustername.full";
    ASSERT_EQ(expectedGroupName, groupName);
    pid.set_taskid("taskId=111-name=incMerge");
    groupName = RoleNameGenerator::generateGroupName(pid);
    expectedGroupName = "merger.clustername.inc";
    ASSERT_EQ(expectedGroupName, groupName);
}

}} // namespace build_service::proto
