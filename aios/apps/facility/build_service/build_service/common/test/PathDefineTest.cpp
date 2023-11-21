#include "build_service/common/PathDefine.h"

#include <iosfwd>
#include <string>

#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/TaskIdentifier.h"
#include "build_service/proto/test/ProtoCreator.h"
#include "build_service/test/unittest.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace build_service::proto;
using namespace build_service::util;

namespace build_service { namespace common {

class PathDefineTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void PathDefineTest::setUp() {}

void PathDefineTest::tearDown() {}

TEST_F(PathDefineTest, testGetPartitionZkRoot)
{
    string zkRoot = "zfs://root";
    {
        PartitionId pid;
        pid.set_role(ROLE_PROCESSOR);
        pid.mutable_buildid()->set_appname("app_name");
        pid.mutable_buildid()->set_datatable("table_name");
        pid.mutable_buildid()->set_generationid(1);
        (*pid.mutable_range()).set_from(10000);
        (*pid.mutable_range()).set_to(20000);
        *pid.add_clusternames() = "cluster1";
        *pid.add_clusternames() = "cluster2";
        *pid.mutable_taskid() = "1";
        ASSERT_EQ("zfs://root/generation.app_name.table_name.1/app_name.table_name.1.processor.full.10000.20000.1",
                  PathDefine::getPartitionZkRoot(zkRoot, pid));
    }
    {
        PartitionId pid;
        pid.set_role(ROLE_BUILDER);
        pid.mutable_buildid()->set_appname("app_name");
        pid.mutable_buildid()->set_datatable("table_name");
        pid.mutable_buildid()->set_appname("app_name");
        pid.mutable_buildid()->set_generationid(1);
        (*pid.mutable_range()).set_from(10000);
        (*pid.mutable_range()).set_to(20000);
        *pid.add_clusternames() = "cluster1";
        ASSERT_EQ("zfs://root/generation.app_name.table_name.1/app_name.table_name.1.builder.full.10000.20000.cluster1",
                  PathDefine::getPartitionZkRoot(zkRoot, pid));
    }
    {
        PartitionId pid;
        pid.set_role(ROLE_MERGER);
        pid.mutable_buildid()->set_appname("app_name");
        pid.mutable_buildid()->set_datatable("table_name");
        pid.mutable_buildid()->set_generationid(1);
        (*pid.mutable_range()).set_from(10000);
        (*pid.mutable_range()).set_to(20000);
        *pid.add_clusternames() = "cluster1";
        pid.set_mergeconfigname("merge_config_name");
        ASSERT_EQ("zfs://root/generation.app_name.table_name.1/"
                  "app_name.table_name.1.merger.merge_config_name.10000.20000.cluster1",
                  PathDefine::getPartitionZkRoot(zkRoot, pid));
    }
    {
        // test pid with backup id
        PartitionId pid;
        pid.set_role(ROLE_PROCESSOR);
        pid.mutable_buildid()->set_appname("app_name");
        pid.mutable_buildid()->set_datatable("table_name");
        pid.mutable_buildid()->set_generationid(1);
        (*pid.mutable_range()).set_from(10000);
        (*pid.mutable_range()).set_to(20000);
        *pid.add_clusternames() = "cluster1";
        *pid.add_clusternames() = "cluster2";
        *pid.mutable_taskid() = "1";
        pid.set_backupid(1);
        ASSERT_EQ(
            "zfs://root/generation.app_name.table_name.1/app_name.table_name.1.processor.full.10000.20000.1.backup.1",
            PathDefine::getPartitionZkRoot(zkRoot, pid));
        ASSERT_EQ("zfs://root/generation.app_name.table_name.1/app_name.table_name.1.processor.full.10000.20000.1",
                  PathDefine::getPartitionZkRoot(zkRoot, pid, true));
    }
    {
        // test agent node
        PartitionId pid;
        pid.set_role(ROLE_AGENT);
        pid.mutable_buildid()->set_appname("app_id");
        pid.mutable_buildid()->set_datatable("table_name");
        pid.mutable_buildid()->set_generationid(1);
        (*pid.mutable_range()).set_from(10000);
        (*pid.mutable_range()).set_to(20000);
        *pid.add_clusternames() = "app_name";
        *pid.mutable_taskid() = "group_name";
        ASSERT_EQ("zfs://root/agent_nodes/app_id.table_name.1.agent.group_name.10000.20000.app_name",
                  PathDefine::getPartitionZkRoot(zkRoot, pid));
    }
}

TEST_F(PathDefineTest, testGetLocalConfigPath)
{
    string configPath = PathDefine::getLocalConfigPath();
    ASSERT_TRUE(configPath != "");
    ASSERT_EQ(fslib::EC_TRUE, fslib::fs::FileSystem::isExist(configPath)) << configPath;
    fslib::util::FileUtil::remove(configPath);
}

TEST_F(PathDefineTest, testGetIndexPartitionLockPath)
{
    string zkRoot = "zfs://root";
    PartitionId pid;
    *pid.add_clusternames() = "cluster";
    (*pid.mutable_range()).set_from(0);
    (*pid.mutable_range()).set_to(65535);
    pid.mutable_buildid()->set_datatable("table_name");
    pid.mutable_buildid()->set_generationid(1);
    ASSERT_EQ("zfs://root/generation..table_name.1.cluster.0.65535.__lock__",
              PathDefine::getIndexPartitionLockPath(zkRoot, pid));
}

TEST_F(PathDefineTest, testAmonitorPath)
{
    PartitionId pid;
    *pid.add_clusternames() = "cluster1";
    *pid.add_clusternames() = "cluster2";
    pid.set_mergeconfigname("inc_merge");
    (*pid.mutable_range()).set_from(123);
    (*pid.mutable_range()).set_to(234);
    pid.mutable_buildid()->set_datatable("table_name");
    pid.mutable_buildid()->set_generationid(1);
    // for processor
    {
        pid.set_role(ROLE_PROCESSOR);
        EXPECT_EQ("generation_1/processor/full/table_name/00123_00234", PathDefine::getAmonitorNodePath(pid));
    }
    // for merger
    {
        pid.clear_clusternames();
        *pid.add_clusternames() = "cluster1";
        pid.set_role(ROLE_BUILDER);
        pid.set_step(BUILD_STEP_INC);
        EXPECT_EQ("generation_1/builder/inc/cluster1/00123_00234", PathDefine::getAmonitorNodePath(pid));
    }
    // for task
    {
        TaskIdentifier identifier;
        identifier.setTaskId("0");
        identifier.setClusterName("cluster1");
        identifier.setTaskName("table_meta_syncronizer");

        PartitionId pid = ProtoCreator::createPartitionId(ROLE_TASK, BUILD_STEP_INC, 0, 65535, "simple", 1, "cluster1",
                                                          "", identifier.toString(), "TaskStateHandlerTest");
        EXPECT_EQ("generation_1/task_name-table_meta_syncronizer/task_id-0/cluster1/00000_65535",
                  PathDefine::getAmonitorNodePath(pid));
    }
    // for builder
    {
        pid.set_role(ROLE_MERGER);
        EXPECT_EQ("generation_1/merger/inc_merge/cluster1/00123_00234", PathDefine::getAmonitorNodePath(pid));
    }
    // test updatable cluster
    {
        pid.clear_clusternames();
        pid.set_step(BUILD_STEP_INC);
        pid.set_role(ROLE_PROCESSOR);
        // for schema updatable cluster
        *pid.mutable_taskid() = "1-clusterName=cluster1";
        EXPECT_EQ("generation_1/processor/inc/table_name/cluster1/00123_00234", PathDefine::getAmonitorNodePath(pid));

        *pid.mutable_taskid() = "1-3-clusterName=cluster2";
        EXPECT_EQ("generation_1/processor/inc/table_name/cluster2/3/00123_00234", PathDefine::getAmonitorNodePath(pid));
        // for default processor group
        *pid.mutable_taskid() = "1";
        EXPECT_EQ("generation_1/processor/inc/table_name/00123_00234", PathDefine::getAmonitorNodePath(pid));
        *pid.mutable_taskid() = "1-3";
        EXPECT_EQ("generation_1/processor/inc/table_name/3/00123_00234", PathDefine::getAmonitorNodePath(pid));
    }
}

}} // namespace build_service::common
