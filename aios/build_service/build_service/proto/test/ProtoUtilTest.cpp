#include "build_service/test/unittest.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/test/ProtoCreator.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace proto {

class ProtoUtilTest : public BUILD_SERVICE_TESTBASE {};

TEST_F(ProtoUtilTest, testPartitionIdToRoleId) {
    PartitionId pid;
    string roleId;

    EXPECT_FALSE(ProtoUtil::partitionIdToRoleId(pid, roleId));
    pid.set_role(ROLE_BUILDER);
    pid.mutable_buildid()->set_appname("app_name");
    pid.mutable_buildid()->set_generationid(1);
    pid.mutable_buildid()->set_datatable("data_table");
    pid.mutable_range()->set_from(0);
    pid.mutable_range()->set_to(65535);
    *pid.add_clusternames() = "mainse_searcher";
    pid.set_mergeconfigname("inc1");
    EXPECT_TRUE(ProtoUtil::partitionIdToRoleId(pid, roleId));
    EXPECT_EQ("app_name.data_table.1.builder.full.0.65535.mainse_searcher", roleId);

    pid.set_role(ROLE_MERGER);
    pid.set_step(BUILD_STEP_INC);
    pid.set_mergeconfigname("inc2");
    EXPECT_TRUE(ProtoUtil::partitionIdToRoleId(pid, roleId));
    EXPECT_EQ("app_name.data_table.1.merger.inc2.0.65535.mainse_searcher", roleId);

    pid.set_taskid("1");
    EXPECT_TRUE(ProtoUtil::partitionIdToRoleId(pid, roleId));
    EXPECT_EQ("app_name.data_table.1.merger.inc2.0.65535.mainse_searcher.1", roleId);
    
    pid.set_taskid("1");
    pid.set_role(ROLE_JOB);
    pid.set_step(BUILD_STEP_FULL);
    EXPECT_TRUE(ProtoUtil::partitionIdToRoleId(pid, roleId));
    EXPECT_EQ("app_name.data_table.1.job.full.0.65535.mainse_searcher.1", roleId);

    *pid.add_clusternames() = "mainse_summary";
    EXPECT_FALSE(ProtoUtil::partitionIdToRoleId(pid, roleId));

    pid.set_role(ROLE_PROCESSOR);
    pid.clear_clusternames();
    pid.set_mergeconfigname("inc3");
    pid.set_step(BUILD_STEP_INC);
    *pid.mutable_taskid() = "1";
    EXPECT_TRUE(ProtoUtil::partitionIdToRoleId(pid, roleId));
    EXPECT_EQ("app_name.data_table.1.processor.inc.0.65535.1", roleId);
}

TEST_F(ProtoUtilTest, testTaskParitionIdRoleId) {
    PartitionId pid = 
        ProtoCreator::createPartitionId(
            ROLE_TASK, NO_BUILD_STEP,
            0, 65535, "simple", 1, "cluster1", "",
            "task_name", "app");
    string pidStr;
    pid.SerializeToString(&pidStr);
    string roleId;
    proto::ProtoUtil::partitionIdToRoleId(pid, roleId);
    PartitionId newId;
    proto::ProtoUtil::roleIdToPartitionId(roleId, "app", newId);
    string newIdStr;
    newId.SerializeToString(&newIdStr);
    
    ASSERT_EQ(newId, pid) << pidStr << ";" << newIdStr;
    string newRoleId;
    proto::ProtoUtil::partitionIdToRoleId(newId, newRoleId);
    ASSERT_EQ(roleId, newRoleId);
    string expectRoleName = "app.simple.1.task.task_name.0.65535.cluster1";
    ASSERT_EQ(expectRoleName, newRoleId);
}

TEST_F(ProtoUtilTest, testRoleIdToPartitionId) {
    {
        PartitionId pid;
        EXPECT_FALSE(ProtoUtil::roleIdToPartitionId("", "", pid));
    }
    {
        PartitionId pid;
        // processor will ignore clusterName
        EXPECT_TRUE(ProtoUtil::roleIdToPartitionId(
                        "app.simple.1.processor.inc.32768.65535.1", "app", pid));
        EXPECT_EQ(ROLE_PROCESSOR, pid.role());
        EXPECT_EQ((uint32_t)1, pid.buildid().generationid());
        EXPECT_EQ((uint32_t)32768, pid.range().from());
        EXPECT_EQ((uint32_t)65535, pid.range().to());
        EXPECT_EQ(0, pid.clusternames_size());
        EXPECT_EQ(BUILD_STEP_INC, pid.step());
    }
    {
        PartitionId pid;
        EXPECT_TRUE(ProtoUtil::roleIdToPartitionId(
                        "app.simple.1.builder.full.0.1.a_b_c", "app", pid));
        EXPECT_EQ(ROLE_BUILDER, pid.role());
        EXPECT_EQ((uint32_t)1, pid.buildid().generationid());
        EXPECT_EQ((uint32_t)0, pid.range().from());
        EXPECT_EQ((uint32_t)1, pid.range().to());
        EXPECT_EQ(1, pid.clusternames_size());
        EXPECT_EQ("a_b_c", pid.clusternames(0));
        EXPECT_EQ(BUILD_STEP_FULL, pid.step());
    }
    {
        PartitionId pid;
        EXPECT_FALSE(ProtoUtil::roleIdToPartitionId(
                        "app.simple.1.builder.full.32768.65535", "app", pid));
    }
    {
        // https://aone.alibaba-inc.com/project/110466/issue/18917653
        PartitionId pid;
        EXPECT_FALSE(ProtoUtil::roleIdToPartitionId("app", "app", pid));
    }
    {
        PartitionId pid;

        EXPECT_TRUE(ProtoUtil::roleIdToPartitionId(
                        "app.simple.1.merger.inc_merge.0.65535.cluster1", "app", pid));
        EXPECT_EQ(ROLE_MERGER, pid.role());
        EXPECT_EQ((uint32_t)1, pid.buildid().generationid());
        EXPECT_EQ((uint32_t)0, pid.range().from());
        EXPECT_EQ((uint32_t)65535, pid.range().to());
        EXPECT_EQ(1, pid.clusternames_size());
        EXPECT_EQ("cluster1", pid.clusternames(0));
        EXPECT_EQ("inc_merge", pid.mergeconfigname());
        EXPECT_EQ(BUILD_STEP_FULL, pid.step());
    }
    {
        PartitionId pid;

        EXPECT_TRUE(ProtoUtil::roleIdToPartitionId(
                        "app.simple.1.merger.inc_merge.0.65535.cluster1.1", "app", pid));
        EXPECT_EQ(ROLE_MERGER, pid.role());
        EXPECT_EQ((uint32_t)1, pid.buildid().generationid());
        EXPECT_EQ((uint32_t)0, pid.range().from());
        EXPECT_EQ((uint32_t)65535, pid.range().to());
        EXPECT_EQ(1, pid.clusternames_size());
        EXPECT_EQ("cluster1", pid.clusternames(0));
        EXPECT_EQ("inc_merge", pid.mergeconfigname());
        EXPECT_EQ("1", pid.taskid());
        EXPECT_EQ(BUILD_STEP_FULL, pid.step());
    }
    {
        PartitionId pid;
        EXPECT_TRUE(ProtoUtil::roleIdToPartitionId(
                        "app.simple.1.merger..0.65535.cluster1", "app", pid));
        EXPECT_EQ(string("app"), pid.buildid().appname());
        EXPECT_EQ(ROLE_MERGER, pid.role());
        EXPECT_EQ((uint32_t)1, pid.buildid().generationid());
        EXPECT_EQ((uint32_t)0, pid.range().from());
        EXPECT_EQ((uint32_t)65535, pid.range().to());
        EXPECT_EQ(1, pid.clusternames_size());
        EXPECT_EQ("cluster1", pid.clusternames(0));
        EXPECT_EQ("", pid.mergeconfigname());
        EXPECT_EQ(BUILD_STEP_FULL, pid.step());
    }
    {
        PartitionId pid;
        EXPECT_TRUE(ProtoUtil::roleIdToPartitionId(
                        "app.simple.1.job.full.0.1234.cluster1.0", "app", pid));
        EXPECT_EQ("app", pid.buildid().appname());
        EXPECT_EQ(ROLE_JOB, pid.role());
        EXPECT_EQ((uint32_t)1, pid.buildid().generationid());
        EXPECT_EQ((uint32_t)0, pid.range().from());
        EXPECT_EQ((uint32_t)1234, pid.range().to());
        EXPECT_EQ(1, pid.clusternames_size());
        EXPECT_EQ("cluster1", pid.clusternames(0));
        EXPECT_EQ("", pid.mergeconfigname());
        EXPECT_EQ(BUILD_STEP_FULL, pid.step());
    }
    {
        PartitionId pid;
        EXPECT_TRUE(ProtoUtil::roleIdToPartitionId(
                        ".simple.1.job.full.0.1234.cluster1.0", "", pid));
        EXPECT_EQ("", pid.buildid().appname());
        EXPECT_EQ(ROLE_JOB, pid.role());
        EXPECT_EQ((uint32_t)1, pid.buildid().generationid());
        EXPECT_EQ((uint32_t)0, pid.range().from());
        EXPECT_EQ((uint32_t)1234, pid.range().to());
        EXPECT_EQ(1, pid.clusternames_size());
        EXPECT_EQ("cluster1", pid.clusternames(0));
        EXPECT_EQ("", pid.mergeconfigname());
        EXPECT_EQ(BUILD_STEP_FULL, pid.step());        
    }
}

TEST_F(ProtoUtilTest, testCheckHeartbeat) {
    PartitionId pid;
    ASSERT_TRUE(ProtoUtil::roleIdToPartitionId(".simple.1.merger.inc_merge.0.65535.cluster1",
                    "", pid));    
    TargetRequest request;
    EXPECT_FALSE(ProtoUtil::checkHeartbeat(request, pid));
    *request.mutable_partitionid() = PartitionId();
    EXPECT_FALSE(ProtoUtil::checkHeartbeat(request, pid));
    EXPECT_DEATH(ProtoUtil::checkHeartbeat(request, pid, true), "role: ROLE_MERGER");
    request.clear_partitionid();
    request.set_roleid("invalid role");
    EXPECT_FALSE(ProtoUtil::checkHeartbeat(request, pid));
    EXPECT_DEATH(ProtoUtil::checkHeartbeat(request, pid, true), "invalid role");
    request.set_roleid("merger..simple.1.0.65535.cluster1");
    EXPECT_FALSE(ProtoUtil::checkHeartbeat(request, pid));
    EXPECT_DEATH(ProtoUtil::checkHeartbeat(request, pid, true), ".*[invalid role] .*[merger.inc_merge.simple.1.0.65535.cluster1].*");
}

TEST_F(ProtoUtilTest, testIsBuildIdEqual) {
    {
        proto::BuildId buildId1 = proto::ProtoCreator::createBuildId("simple1", 1, "app1");
        proto::BuildId buildId2 = proto::ProtoCreator::createBuildId("simple1", 1, "app2");
        EXPECT_FALSE(ProtoUtil::isEqual(buildId1, buildId2));        
    }
    {
        proto::BuildId buildId1 = proto::ProtoCreator::createBuildId("simple1", 1, "app1");
        proto::BuildId buildId2 = proto::ProtoCreator::createBuildId("simple1", 2, "app1");
        EXPECT_FALSE(ProtoUtil::isEqual(buildId1, buildId2));        
    }
    {
        proto::BuildId buildId1 = proto::ProtoCreator::createBuildId("simple1", 1, "app1");
        proto::BuildId buildId2 = proto::ProtoCreator::createBuildId("simple2", 1, "app1");
        EXPECT_FALSE(ProtoUtil::isEqual(buildId1, buildId2));        
    }
    {
        proto::BuildId buildId1 = proto::ProtoCreator::createBuildId("simple1", 1, "app1");
        proto::BuildId buildId2 = proto::ProtoCreator::createBuildId("simple1", 1, "app1");
        EXPECT_TRUE(ProtoUtil::isEqual(buildId1, buildId2));        
    }
}
    

}
}
