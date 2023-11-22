#include "build_service/common/CounterSynchronizer.h"

#include <iosfwd>
#include <string>

#include "autil/TimeUtility.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::proto;

namespace build_service { namespace common {

class CounterSynchronizerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void CounterSynchronizerTest::setUp() {}

void CounterSynchronizerTest::tearDown() {}

TEST_F(CounterSynchronizerTest, testGetCounterRedisKey)
{
    string serviceName = "bsApp";
    {
        PartitionId pid;
        pid.set_role(ROLE_PROCESSOR);
        pid.set_step(BUILD_STEP_FULL);
        pid.mutable_range()->set_from(0);
        pid.mutable_range()->set_to(32767);
        pid.mutable_buildid()->set_appname("app");
        pid.mutable_buildid()->set_generationid(666);
        pid.mutable_buildid()->set_datatable("simple");
        *pid.add_clusternames() = "cluster1";

        string redisKey = CounterSynchronizer::getCounterRedisKey(serviceName, pid.buildid());
        string fieldName;
        ASSERT_TRUE(CounterSynchronizer::getCounterRedisHashField(pid, fieldName));
        ASSERT_EQ(string("bsApp.app.simple.666"), redisKey);
        ASSERT_EQ(string("processor.full.0.32767"), fieldName);
    }
    {
        PartitionId pid;
        pid.set_role(ROLE_BUILDER);
        pid.set_step(BUILD_STEP_FULL);
        pid.mutable_range()->set_from(0);
        pid.mutable_range()->set_to(32767);
        pid.mutable_buildid()->set_appname("app");
        pid.mutable_buildid()->set_generationid(666);
        pid.mutable_buildid()->set_datatable("simple");
        *pid.add_clusternames() = "cluster1";

        string redisKey = CounterSynchronizer::getCounterRedisKey(serviceName, pid.buildid());
        string fieldName;
        ASSERT_TRUE(CounterSynchronizer::getCounterRedisHashField(pid, fieldName));
        ASSERT_EQ(string("bsApp.app.simple.666"), redisKey);
        ASSERT_EQ(string("builder.full.0.32767.cluster1"), fieldName);
    }
    {
        PartitionId pid;
        pid.set_role(ROLE_MERGER);
        pid.set_step(BUILD_STEP_FULL);
        pid.mutable_range()->set_from(0);
        pid.mutable_range()->set_to(32767);
        pid.mutable_buildid()->set_appname("app");
        pid.mutable_buildid()->set_generationid(666);
        pid.mutable_buildid()->set_datatable("simple");
        *pid.add_clusternames() = "cluster1";
        pid.set_mergeconfigname("inc_1");

        string redisKey = CounterSynchronizer::getCounterRedisKey(serviceName, pid.buildid());
        string fieldName;
        ASSERT_TRUE(CounterSynchronizer::getCounterRedisHashField(pid, fieldName));
        ASSERT_EQ(string("bsApp.app.simple.666"), redisKey);
        ASSERT_EQ(string("merger.inc_1.0.32767.cluster1"), fieldName);
    }
}

TEST_F(CounterSynchronizerTest, testGetCounterZkFilePath)
{
    string appZkRoot = "zfs://testAppZkRoot";
    PartitionId pid;
    pid.mutable_range()->set_from(0);
    pid.mutable_range()->set_to(32767);
    pid.mutable_buildid()->set_appname("app");
    pid.mutable_buildid()->set_generationid(666);
    pid.mutable_buildid()->set_datatable("simple");

    {
        pid.set_role(ROLE_PROCESSOR);
        pid.set_step(BUILD_STEP_FULL);
        string counterFilePath;
        ASSERT_TRUE(CounterSynchronizer::getCounterZkFilePath(appZkRoot, pid, counterFilePath));

        EXPECT_EQ(string("zfs://testAppZkRoot/generation.app.simple.666/counter/app.simple.666.processor.full.0.32767"),
                  counterFilePath);
    }
    {
        pid.set_role(ROLE_BUILDER);
        pid.set_step(BUILD_STEP_FULL);
        *pid.add_clusternames() = "cluster1";
        string counterFilePath;
        ASSERT_TRUE(CounterSynchronizer::getCounterZkFilePath(appZkRoot, pid, counterFilePath));
        EXPECT_EQ(
            string(
                "zfs://testAppZkRoot/generation.app.simple.666/counter/app.simple.666.builder.full.0.32767.cluster1"),
            counterFilePath);
    }
    {
        pid.set_role(ROLE_MERGER);
        pid.set_step(BUILD_STEP_FULL);
        pid.clear_clusternames();
        *pid.add_clusternames() = "cluster1";
        pid.set_mergeconfigname("inc_1");
        string counterFilePath;
        ASSERT_TRUE(CounterSynchronizer::getCounterZkFilePath(appZkRoot, pid, counterFilePath));
        EXPECT_EQ(
            string(
                "zfs://testAppZkRoot/generation.app.simple.666/counter/app.simple.666.merger.inc_1.0.32767.cluster1"),
            counterFilePath);
    }
    {
        PartitionId backupPid = pid;
        backupPid.set_role(ROLE_PROCESSOR);
        backupPid.set_step(BUILD_STEP_FULL);
        backupPid.set_backupid(1);
        string counterFilePath;
        ASSERT_TRUE(CounterSynchronizer::getCounterZkFilePath(appZkRoot, backupPid, counterFilePath));
        EXPECT_EQ(
            "zfs://testAppZkRoot/generation.app.simple.666/counter/app.simple.666.processor.full.0.32767.backup.1",
            counterFilePath);
        ASSERT_TRUE(CounterSynchronizer::getCounterZkFilePath(appZkRoot, backupPid, counterFilePath, true));
        EXPECT_EQ("zfs://testAppZkRoot/generation.app.simple.666/counter/app.simple.666.processor.full.0.32767",
                  counterFilePath);
    }
    // test for schema updatable cluster
    {
        // for schema updatable cluster
        pid.set_role(ROLE_PROCESSOR);
        pid.set_step(BUILD_STEP_INC);
        pid.clear_clusternames();
        *pid.mutable_taskid() = "1-clusterName=cluster1";
        string counterFilePath;
        ASSERT_TRUE(CounterSynchronizer::getCounterZkFilePath(appZkRoot, pid, counterFilePath));
        EXPECT_EQ(
            string(
                "zfs://testAppZkRoot/generation.app.simple.666/counter/app.simple.666.processor.inc.0.32767.cluster1"),
            counterFilePath);

        *pid.mutable_taskid() = "1-2-clusterName=cluster1";
        ASSERT_TRUE(CounterSynchronizer::getCounterZkFilePath(appZkRoot, pid, counterFilePath));
        EXPECT_EQ(string("zfs://testAppZkRoot/generation.app.simple.666/counter/"
                         "app.simple.666.processor.inc.0.32767.cluster1.2"),
                  counterFilePath);

        // for default processor group
        *pid.mutable_taskid() = "1";
        ASSERT_TRUE(CounterSynchronizer::getCounterZkFilePath(appZkRoot, pid, counterFilePath));
        EXPECT_EQ(string("zfs://testAppZkRoot/generation.app.simple.666/counter/app.simple.666.processor.inc.0.32767"),
                  counterFilePath);

        *pid.mutable_taskid() = "1-3";
        ASSERT_TRUE(CounterSynchronizer::getCounterZkFilePath(appZkRoot, pid, counterFilePath));
        EXPECT_EQ(
            string("zfs://testAppZkRoot/generation.app.simple.666/counter/app.simple.666.processor.inc.0.32767.3"),
            counterFilePath);

        // for empty taskid
        *pid.mutable_taskid() = "";
        ASSERT_TRUE(CounterSynchronizer::getCounterZkFilePath(appZkRoot, pid, counterFilePath));
        EXPECT_EQ(string("zfs://testAppZkRoot/generation.app.simple.666/counter/app.simple.666.processor.inc.0.32767"),
                  counterFilePath);
    }
}

}} // namespace build_service::common
