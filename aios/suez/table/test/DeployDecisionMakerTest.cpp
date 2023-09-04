#include "suez/table/DeployDecisionMaker.h"

#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class DeployDecisionMakerTest : public TESTBASE {};

TEST_F(DeployDecisionMakerTest, testComputeEvent) {
    DeployDecisionMaker maker;
    PartitionMeta current = TableMetaUtil::make(0, TS_UNKNOWN, DS_UNKNOWN, "path/to/index");
    ;
    auto target = TableMetaUtil::make(0, TS_UNKNOWN, DS_UNKNOWN, "path/to/index");

    auto type = maker.computeEvent(current, target, true, false);
    EXPECT_EQ(ET_DEPLOY_NONE, type);

    target.incVersion = 1;
    type = maker.computeEvent(current, target, true, false);
    EXPECT_EQ(ET_NEW_INC_VERSION, type);

    target.incVersion = 0;
    current.setConfigPath("a");
    target.setConfigPath("b");
    type = maker.computeEvent(current, target, true, false);
    EXPECT_EQ(ET_NEW_CONFIG, type);
    type = maker.computeEvent(current, target, false, false);
    EXPECT_EQ(ET_DEPLOY_NONE, type);
    current.setConfigPath("b");

    current.setIndexRoot("a");
    target.setIndexRoot("b");
    type = maker.computeEvent(current, target, true, true);
    EXPECT_EQ(ET_INDEX_ROOT_CHANGED, type);
    type = maker.computeEvent(current, target, true, false);
    EXPECT_EQ(ET_DEPLOY_NONE, type);
    current.setIndexRoot("a");

    target.incVersion = 1;
    current.setDeployStatus(0, DS_DEPLOYING);
    type = maker.computeEvent(current, target, false, false);
    EXPECT_EQ(ET_WAIT_RESOURCE, type);
}

TEST_F(DeployDecisionMakerTest, testSimple) {
    DeployDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_UNKNOWN, "path/to/index");

    auto op = maker.makeDecision(current, target, true, false, false);
    ASSERT_EQ(OP_DEPLOY, op);

    op = maker.makeDecision(current, target, true, false, true);
    ASSERT_EQ(OP_DEPLOY, op);

    current.setDeployStatus(1, DS_DEPLOYING);
    op = maker.makeDecision(current, target, true, false, false);
    ASSERT_EQ(OP_HOLD, op);
    op = maker.makeDecision(current, target, true, false, true);
    ASSERT_EQ(OP_HOLD, op);

    current.setDeployStatus(1, DS_FAILED);
    op = maker.makeDecision(current, target, true, false, false);
    ASSERT_EQ(OP_DEPLOY, op);
    op = maker.makeDecision(current, target, true, false, true);
    ASSERT_EQ(OP_DEPLOY, op);

    current.setDeployStatus(1, DS_CANCELLED);
    op = maker.makeDecision(current, target, true, false, false);
    ASSERT_EQ(OP_DEPLOY, op);
    op = maker.makeDecision(current, target, true, false, true);
    ASSERT_EQ(OP_DEPLOY, op);

    current.setDeployStatus(1, DS_DEPLOYDONE);
    op = maker.makeDecision(current, target, true, false, false);
    ASSERT_EQ(OP_NONE, op);
    op = maker.makeDecision(current, target, true, false, true);
    ASSERT_EQ(OP_NONE, op);

    current.setDeployStatus(1, DS_DISKQUOTA);
    op = maker.makeDecision(current, target, true, false, false);
    ASSERT_EQ(OP_CLEAN_DISK, op);
    op = maker.makeDecision(current, target, true, false, true);
    ASSERT_EQ(OP_HOLD, op);

    target.setIncVersion(2);
    op = maker.makeDecision(current, target, true, false, false);
    ASSERT_EQ(OP_DEPLOY, op);
    op = maker.makeDecision(current, target, true, false, true);
    ASSERT_EQ(OP_DEPLOY, op);

    current.setDeployStatus(1, DS_DEPLOYING);
    op = maker.makeDecision(current, target, true, false, false);
    ASSERT_EQ(OP_CANCELDEPLOY, op);
    op = maker.makeDecision(current, target, true, false, true);
    ASSERT_EQ(OP_CANCELDEPLOY, op);
}

TEST_F(DeployDecisionMakerTest, testConfigChanged) {
    {
        DeployDecisionMaker maker;
        auto current = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");
        auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");
        target.setConfigPath("fake_config/1483473856");

        auto op = maker.makeDecision(current, target, true, false, false);
        ASSERT_EQ(OP_DIST_DEPLOY, op);

        op = maker.makeDecision(current, target, true, false, true);
        ASSERT_EQ(OP_HOLD, op);
    }
    {
        DeployDecisionMaker maker;
        auto current = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");
        auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");
        target.setConfigPath("fake_config/1483473856");

        auto op = maker.makeDecision(current, target, false, false, false);
        ASSERT_EQ(OP_NONE, op);

        op = maker.makeDecision(current, target, false, false, true);
        ASSERT_EQ(OP_NONE, op);
    }
}

TEST_F(DeployDecisionMakerTest, testIndexRootChanged) {
    {
        DeployDecisionMaker maker;
        auto current = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index1");
        auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index2");

        auto op = maker.makeDecision(current, target, false, true, false);
        ASSERT_EQ(OP_DIST_DEPLOY, op);

        op = maker.makeDecision(current, target, false, true, true);
        ASSERT_EQ(OP_HOLD, op);
    }
    {
        DeployDecisionMaker maker;
        auto current = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index1");
        auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index2");

        auto op = maker.makeDecision(current, target, false, false, false);
        ASSERT_EQ(OP_NONE, op);

        op = maker.makeDecision(current, target, false, false, true);
        ASSERT_EQ(OP_NONE, op);
    }
}

TEST_F(DeployDecisionMakerTest, testFinalTargetDeployWithConfigChanged) {
    DeployDecisionMaker maker;
    auto current = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index1");
    auto target = TableMetaUtil::make(2, TS_UNKNOWN, DS_UNKNOWN, "path/to/index2");
    auto op = maker.makeDecision(current, target, false, false, false);
    ASSERT_EQ(OP_DEPLOY, op);

    op = maker.makeDecision(current, target, false, false, true);
    ASSERT_EQ(OP_HOLD, op);

    current.setIndexRoot("/path/to/index2");
    current.setConfigPath("/path/to/current");
    target.setConfigPath("/path/to/target");
    op = maker.makeDecision(current, target, false, false, false);
    ASSERT_EQ(OP_DEPLOY, op);
    op = maker.makeDecision(current, target, false, false, true);
    ASSERT_EQ(OP_HOLD, op);
}

TEST_F(DeployDecisionMakerTest, testDeploying) {
    DeployDecisionMaker maker;
    auto current = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYING, "/path/to/index1");
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_UNKNOWN, "/path/to/index1");
    auto op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_HOLD, op);
    target.setConfigPath("/path/to/config/2");
    op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_HOLD, op);
    current.setConfigPath("/path/to/config/2");
    target.setIndexRoot("/path/to/index/2");
    op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_HOLD, op);

    target.incVersion = 2;
    current.setDeployStatus(1, DS_DEPLOYDONE);
    current.setDeployStatus(2, DS_DEPLOYING);
    op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_HOLD, op);
}

TEST_F(DeployDecisionMakerTest, testDeployDone) {
    DeployDecisionMaker maker;
    auto current = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "/path/to/index1");
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_UNKNOWN, "/path/to/index1");
    auto op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_NONE, op);

    target.setConfigPath("new_config_path");
    op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_DIST_DEPLOY, op);

    target.setConfigPath(current.getConfigPath());
    target.setIndexRoot("new_index_root");
    op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_DIST_DEPLOY, op);

    target.setIndexRoot(current.getIndexRoot());
    current.setDeployStatus(2, DS_DEPLOYING);
    op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_NONE, op);

    current.setDeployStatus(2, DS_DEPLOYDONE);
    op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_NONE, op);
}

TEST_F(DeployDecisionMakerTest, testDiskQuota) {
    DeployDecisionMaker maker;
    auto current = TableMetaUtil::make(1, TS_UNKNOWN, DS_DISKQUOTA, "/path/to/index1");
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_UNKNOWN, "/path/to/index1");
    auto op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_CLEAN_DISK, op);
    target.setIndexRoot("/path/to/index2");
    op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_CLEAN_DISK, op);
    current.setIndexRoot("/path/to/index2");
    target.setConfigPath("/path/to/config2");
    op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_CLEAN_DISK, op);
    current.setConfigPath("/path/to/config2");
    current.setDeployStatus(2, DS_DEPLOYING);
    op = maker.makeDecision(current, target, true, true, false);
    ASSERT_EQ(OP_CLEAN_DISK, op);
}

} // namespace suez
