#include "suez/table/LoadDecisionMaker.h"

#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class LoadDecisionMakerTest : public TESTBASE {
public:
    void setUp() override {
        _ctx.isFinal = false;
        _ctx.supportPreload = false;
    }

protected:
    void doTestRoleChange(RoleType targetRole, RoleType currentRole, OperationType expectedOp);

protected:
    LoadDecisionMaker::Context _ctx;
};

TEST_F(LoadDecisionMakerTest, testDetermineLoadType) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    current.incVersion = 0;
    ScheduleConfig options;
    auto target = TableMetaUtil::make(0, TS_UNKNOWN, DS_UNKNOWN, "path/to/index");

    auto type = maker.determineLoadType(current, target, options);
    EXPECT_EQ(LT_NONE, type);

    target.incVersion = 1;
    type = maker.determineLoadType(current, target, options);
    EXPECT_EQ(LT_LOAD, type);

    current.setLoadedConfigPath("a");
    target.setConfigPath("b");
    type = maker.determineLoadType(current, target, options);
    EXPECT_EQ(LT_LOAD, type);
    options.allowReloadByConfig = true;
    type = maker.determineLoadType(current, target, options);
    EXPECT_EQ(LT_RELOAD, type);

    current.setLoadedConfigPath("b");
    current.setLoadedIndexRoot("a");
    target.setIndexRoot("b");
    type = maker.determineLoadType(current, target, options);
    EXPECT_EQ(LT_LOAD, type);
    options.allowReloadByIndexRoot = true;
    type = maker.determineLoadType(current, target, options);
    EXPECT_EQ(LT_RELOAD, type);
    current.setLoadedIndexRoot("b");

    target.incVersion = -1;
    type = maker.determineLoadType(current, target, options);
    EXPECT_EQ(LT_RELOAD, type);
}

TEST_F(LoadDecisionMakerTest, testDetermineLoadTypeWithRoleChange) {
    LoadDecisionMaker maker;
    ScheduleConfig options;
    PartitionMeta target, current;

    target.setRoleType(RT_LEADER);
    current.setRoleType(RT_FOLLOWER);
    EXPECT_EQ(LT_BECOME_LEADER, maker.determineLoadType(current, target, options));

    target.setRoleType(RT_FOLLOWER);
    current.setRoleType(RT_LEADER);
    EXPECT_EQ(LT_NO_LONGER_LEADER, maker.determineLoadType(current, target, options));
}

TEST_F(LoadDecisionMakerTest, testInit) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_UNKNOWN, "path/to/index");

    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_INIT, op);
    current.setTableStatus(TS_INITIALIZING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);
}

TEST_F(LoadDecisionMakerTest, testForceLoad) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");

    current.setTableStatus(TS_ERROR_LACK_MEM);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FORCELOAD, op);

    current.setTableStatus(TS_ERROR_UNKNOWN);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FORCELOAD, op);

    current.setTableStatus(TS_PRELOAD_FAILED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);
}

TEST_F(LoadDecisionMakerTest, testForceReload) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");

    current.setTableStatus(TS_FORCE_RELOAD);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);
}

TEST_F(LoadDecisionMakerTest, testConfigChangeReload) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");

    target.deployMeta->configPath = "newConfigPath";
    current.setTableStatus(TS_LOADED);
    current.incVersion = 1;
    current.deployMeta->configPath = "newConfigPath";
    current.loadedConfigPath = "oldConfigPath";

    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    _ctx.config.allowReloadByConfig = true;
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.incVersion = -1;
    _ctx.config.allowReloadByConfig = false;
    // not allowReloadByConfig, only do load
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    _ctx.config.allowReloadByConfig = true;
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_UNKNOWN);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_INIT, op);

    current.setTableStatus(TS_UNLOADED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    // error_config only load
    current.setTableStatus(TS_ERROR_CONFIG);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    // other error need reload
    current.setTableStatus(TS_ERROR_LACK_MEM);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_ERROR_UNKNOWN);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_PRELOAD_FAILED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);

    current.setTableStatus(TS_FORCE_RELOAD);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_PRELOAD_FORCE_RELOAD);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);
}

TEST_F(LoadDecisionMakerTest, testIndexRootChangeReload) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");

    current.setTableStatus(TS_LOADED);
    current.setIndexRoot("newIndexRoot");
    current.setLoadedIndexRoot("oldIndexRoot");
    current.incVersion = 1;
    target.deployMeta->indexRoot = "newIndexRoot";

    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    _ctx.config.allowReloadByIndexRoot = true;
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.incVersion = -1;
    _ctx.config.allowReloadByIndexRoot = false;
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    _ctx.config.allowReloadByIndexRoot = true;
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_UNKNOWN);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_INIT, op);

    current.setTableStatus(TS_UNLOADED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    // error_config only load
    current.setTableStatus(TS_ERROR_CONFIG);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    // other error need reload
    current.setTableStatus(TS_ERROR_LACK_MEM);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_ERROR_UNKNOWN);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_PRELOAD_FAILED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);

    current.setTableStatus(TS_FORCE_RELOAD);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_PRELOAD_FORCE_RELOAD);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);
}

TEST_F(LoadDecisionMakerTest, testLoadUnload) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");

    // load
    current.setTableStatus(TS_UNLOADED);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    current.setTableStatus(TS_ERROR_CONFIG);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    // when loading
    current.setTableStatus(TS_LOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    // // when loaded
    current.setTableStatus(TS_LOADED);
    current.incVersion = 1;
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    // // normal load inc
    target.incVersion = 2;
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    // // force load inc
    current.setTableStatus(TS_ERROR_LACK_MEM);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FORCELOAD, op);

    current.setTableStatus(TS_ERROR_UNKNOWN);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FORCELOAD, op);

    // rollback
    target.incVersion = 0;
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    // unloading
    current.setTableStatus(TS_UNLOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_COMMITTING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_HOLD, op);

    current.setTableStatus(TS_COMMIT_ERROR);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    // loaded  when versionId is same but branchId is diff
    current.setTableStatus(TS_LOADED);
    current.incVersion = 1;
    current.branchId = 12345;
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);
}

TEST_F(LoadDecisionMakerTest, testFinalNoPreload) {
    _ctx.isFinal = true;
    _ctx.supportPreload = false;

    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");

    current.setTableStatus(TS_UNKNOWN);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    // load
    current.setTableStatus(TS_LOADED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_UNLOADED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_ERROR_CONFIG);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);
}

TEST_F(LoadDecisionMakerTest, testPreload) {
    _ctx.isFinal = true;
    _ctx.supportPreload = true;
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");

    current.setTableStatus(TS_UNKNOWN);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_INIT, op);

    // load
    current.setTableStatus(TS_LOADED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_PRELOAD, op);

    current.setTableStatus(TS_UNLOADED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_PRELOAD, op);

    current.setTableStatus(TS_ERROR_CONFIG);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_PRELOAD, op);
}

TEST_F(LoadDecisionMakerTest, testAllowFroceLoadOption) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");
    _ctx.config.allowForceLoad = false;

    current.setTableStatus(TS_ERROR_LACK_MEM);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    current.setTableStatus(TS_ERROR_UNKNOWN);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    current.setTableStatus(TS_PRELOAD_FAILED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);

    current.setTableStatus(TS_UNLOADED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    // reload
    target.incVersion = 0;

    current.setTableStatus(TS_UNLOADED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    current.setTableStatus(TS_LOADED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    current.setTableStatus(TS_ERROR_LACK_MEM);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    current.setTableStatus(TS_ERROR_UNKNOWN);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    current.setTableStatus(TS_PRELOAD_FAILED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);

    current.setTableStatus(TS_FORCE_RELOAD);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    current.setTableStatus(TS_PRELOAD_FORCE_RELOAD);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);
}

TEST_F(LoadDecisionMakerTest, testNoneOpertion) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");

    // load
    current.setTableStatus(TS_UNLOADED);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_LOAD, op);

    current.setTableStatus(TS_LOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_UNLOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_FORCELOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_PRELOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);

    // reload
    current.incVersion = 1;
    target.incVersion = 0;
    current.setTableStatus(TS_LOADED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_COMMITTING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_HOLD, op);

    current.setTableStatus(TS_COMMIT_ERROR);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_LOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_UNLOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_FORCELOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_PRELOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);

    // reload by error
    current.incVersion = 1;
    target.incVersion = 1;
    current.setTableStatus(TS_COMMIT_ERROR);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);

    current.setTableStatus(TS_ROLE_SWITCH_ERROR);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);
}

TEST_F(LoadDecisionMakerTest, testFinalTargetToTarget) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");

    current.setTableStatus(TS_PRELOADING);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);

    current.setTableStatus(TS_PRELOAD_FAILED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);

    current.setTableStatus(TS_PRELOAD_FORCE_RELOAD);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_FINAL_TO_TARGET, op);

    _ctx.isFinal = true;
    _ctx.supportPreload = true;

    current.setTableStatus(TS_PRELOADING);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_PRELOAD_FAILED);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);

    current.setTableStatus(TS_PRELOAD_FORCE_RELOAD);
    op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_NONE, op);
}

void LoadDecisionMakerTest::doTestRoleChange(RoleType targetRole, RoleType currentRole, OperationType expectedOp) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");

    current.setRoleType(currentRole);
    target.setRoleType(targetRole);

    current.setTableStatus(TS_UNKNOWN);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_INIT, op);

    std::vector<TableStatus> loadingStatus = {
        TS_LOADING,
        TS_PRELOADING,
    };
    for (auto status : loadingStatus) {
        current.setTableStatus(status);
        op = maker.makeDecision(current, target, _ctx);
        ASSERT_EQ(OP_CANCELLOAD, op);
    }

    std::vector<TableStatus> uncancelableStatus = {TS_UNLOADING, TS_COMMITTING, TS_FORCELOADING};
    for (auto status : uncancelableStatus) {
        current.setTableStatus(status);
        op = maker.makeDecision(current, target, _ctx);
        ASSERT_EQ(OP_HOLD, op);
    }

    std::vector<TableStatus> otherStatus = {TS_UNLOADED,
                                            TS_LOADED,
                                            TS_FORCE_RELOAD,
                                            TS_PRELOAD_FAILED,
                                            TS_PRELOAD_FORCE_RELOAD,
                                            TS_ERROR_LACK_MEM,
                                            TS_ERROR_CONFIG,
                                            TS_ERROR_UNKNOWN,
                                            TS_COMMIT_ERROR};
    for (auto status : otherStatus) {
        current.setTableStatus(status);
        op = maker.makeDecision(current, target, _ctx);
        ASSERT_EQ(expectedOp, op);
    }
}

TEST_F(LoadDecisionMakerTest, testBecomeLeader) { doTestRoleChange(RT_LEADER, RT_FOLLOWER, OP_BECOME_LEADER); }

TEST_F(LoadDecisionMakerTest, testNolongerLeader) { doTestRoleChange(RT_FOLLOWER, RT_LEADER, OP_NO_LONGER_LEADER); }

TEST_F(LoadDecisionMakerTest, testRoleSwitching) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    current.setTableStatus(TS_ROLE_SWITCHING);
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");
    current.setRoleType(RT_LEADER);
    target.setRoleType(RT_FOLLOWER);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_HOLD, op);
}

TEST_F(LoadDecisionMakerTest, testRoleSwitchError) {
    LoadDecisionMaker maker;
    PartitionMeta current;
    current.setTableStatus(TS_ROLE_SWITCH_ERROR);
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_DEPLOYDONE, "path/to/index");
    current.setRoleType(RT_LEADER);
    target.setRoleType(RT_FOLLOWER);
    auto op = maker.makeDecision(current, target, _ctx);
    ASSERT_EQ(OP_RELOAD, op);
}

} // namespace suez
