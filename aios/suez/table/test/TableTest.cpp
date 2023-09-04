#include "suez/table/Table.h"

#include "MockLeaderElectionManager.h"
#include "MockSuezPartition.h"
#include "MockSuezPartitionFactory.h"
#include "TableTestUtil.h"
#include "suez/common/test/TableMetaUtil.h"
#include "suez/sdk/TableWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class TableTest : public TESTBASE {
public:
    void setUp() override { _factory = std::make_unique<NiceMockSuezPartitionFactory>(); }

protected:
    void checkTableStatus(const Table &t, TableStatus expected) { ASSERT_EQ(expected, t.getTableStatus()); }
    void checkDeployStatus(const Table &t, IncVersion version, DeployStatus expected) {
        ASSERT_EQ(expected, t.getTableDeployStatus(version));
    }
    void checkError(const Table &t, const SuezError &expected) { ASSERT_EQ(expected, t._partitionMeta->getError()); }

    std::unique_ptr<Table> makeInitedTable() {
        TableResource res(TableMetaUtil::makePid("t"));
        auto t = std::make_unique<Table>(res, _factory.get());
        t->setTableStatus(TS_UNLOADED);
        _partition = new NiceMockSuezPartition(t->_partitionMeta, t->getPid(), res.metricsReporter);
        t->_suezPartition.reset(_partition);
        return t;
    }

    std::unique_ptr<Table> makeDeployedTable(IncVersion inc) {
        auto t = makeInitedTable();
        t->_partitionMeta->setDeployStatus(inc, DS_DEPLOYDONE);
        return t;
    }

    std::unique_ptr<Table> makeLoadedTable(IncVersion inc) {
        auto t = makeDeployedTable(inc);
        t->setTableStatus(TS_LOADED);
        return t;
    }

    void doTestRoleSwitch(RoleType current, RoleType target, bool allowFollowerWrite = false);

protected:
    std::unique_ptr<NiceMockSuezPartitionFactory> _factory;
    NiceMockSuezPartition *_partition = nullptr;
};

TEST_F(TableTest, testConstruct) {
    Table t(TableResource(TableMetaUtil::makePid("t")));
    ASSERT_EQ(TS_UNKNOWN, t.getTableStatus());
    ASSERT_TRUE(t._factory != nullptr);
    ASSERT_TRUE(t._ownFactory);
}

TEST_F(TableTest, testInit) {
    Table t(TableResource(TableMetaUtil::makePid("t")), _factory.get());
    auto fn = [this, &t](SuezPartitionType type,
                         const TableResource &res,
                         const CurrentPartitionMetaPtr &current) -> SuezPartitionPtr {
        checkTableStatus(t, TS_INITIALIZING);
        return SuezPartitionPtr(new NiceMockSuezPartition(current, res.pid, res.metricsReporter));
    };
    EXPECT_CALL(*_factory, create(_, _, _)).WillOnce(Invoke(std::move(fn)));

    auto target = TableMetaUtil::makeWithTableType(1);
    target.setTableMode(TM_READ_WRITE);
    target.setTotalPartitionCount(1);
    t.init(target);
    ASSERT_EQ(TS_UNLOADED, t.getTableStatus());
    ASSERT_EQ(TM_READ_WRITE, t.getPartitionMeta().getTableMode());
    ASSERT_EQ(0, t.getPid().index);
}

TEST_F(TableTest, testInitFailed) {
    Table t(TableResource(TableMetaUtil::makePid("t")), _factory.get());
    auto fn = [this, &t](SuezPartitionType type,
                         const TableResource &res,
                         const CurrentPartitionMetaPtr &current) -> SuezPartitionPtr {
        checkTableStatus(t, TS_INITIALIZING);
        return SuezPartitionPtr();
    };
    EXPECT_CALL(*_factory, create(_, _, _)).WillOnce(Invoke(std::move(fn)));

    auto target = TableMetaUtil::makeWithTableType(1);
    t.init(target);
    EXPECT_EQ(TS_UNKNOWN, t.getTableStatus());
    EXPECT_TRUE(t._partitionMeta->getError() == CREATE_TABLE_ERROR);
}

#define deployFn(ret)                                                                                                  \
    ({                                                                                                                 \
        auto fn = [ret, this, &table](const TargetPartitionMeta &target, bool dist) -> StatusAndError<DeployStatus> {  \
            checkDeployStatus(*table, target.getIncVersion(), DS_DEPLOYING);                                           \
            return ret;                                                                                                \
        };                                                                                                             \
        fn;                                                                                                            \
    })

TEST_F(TableTest, testDeploy) {
    auto table = makeInitedTable();
    auto target = TableMetaUtil::makeTarget(1, "/index/root", "/config/root");
    auto dpStatusVec = vector<StatusAndError<DeployStatus>>{
        StatusAndError<DeployStatus>(DS_DEPLOYDONE),
        StatusAndError<DeployStatus>(DS_FAILED, DEPLOY_CONFIG_ERROR),
        StatusAndError<DeployStatus>(DS_DISKQUOTA, DEPLOY_DISK_QUOTA_ERROR),
        StatusAndError<DeployStatus>(DS_CANCELLED, ERROR_NONE),
    };
    for (auto expected : dpStatusVec) {
        auto fn = deployFn(expected);
        EXPECT_CALL(*_partition, deploy(_, _)).WillOnce(Invoke(std::move(fn)));
        table->deploy(target, false);
        checkDeployStatus(*table, 1, expected.status);
        checkError(*table, expected.error);
    }
}

#undef deployFn

#define loadFn(ret, preTs)                                                                                             \
    ({                                                                                                                 \
        auto fn = [ret, this, &table](const TargetPartitionMeta &target, bool force) -> StatusAndError<TableStatus> {  \
            checkTableStatus(*table, preTs);                                                                           \
            return ret;                                                                                                \
        };                                                                                                             \
        fn;                                                                                                            \
    })

TEST_F(TableTest, testLoad) {
    auto table = makeDeployedTable(1);
    auto target = TableMetaUtil::makeTarget(1, "/index/root", "/config/root");
    auto tsStatusVec = vector<StatusAndError<TableStatus>>{
        StatusAndError<TableStatus>(TS_LOADED),
        StatusAndError<TableStatus>(TS_ERROR_UNKNOWN),
        StatusAndError<TableStatus>(TS_ERROR_CONFIG),
        StatusAndError<TableStatus>(TS_ERROR_LACK_MEM),
        StatusAndError<TableStatus>(TS_FORCE_RELOAD),
    };

    for (auto expected : tsStatusVec) {
        auto fn = loadFn(expected, TS_LOADING);
        EXPECT_CALL(*_partition, load(_, _)).WillOnce(Invoke(std::move(fn)));
        table->load(target);
        EXPECT_EQ(expected.status, table->getTableStatus());
        checkError(*table, expected.error);
    }
}

TEST_F(TableTest, testForceLoad) {
    auto table = makeDeployedTable(1);
    auto target = TableMetaUtil::makeTarget(1, "/index/root", "/config/root");
    auto tsStatusVec = vector<StatusAndError<TableStatus>>{
        StatusAndError<TableStatus>(TS_LOADED),
        StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_UNKNOWN_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_FORCE_REOPEN_UNKNOWN_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_CONFIG, CONFIG_LOAD_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_LACK_MEM, LOAD_LACKMEM_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_LACK_MEM, LOAD_FORCE_REOPEN_LACKMEM_ERROR),
        StatusAndError<TableStatus>(TS_FORCE_RELOAD, FORCE_RELOAD_ERROR),
    };

    for (auto expected : tsStatusVec) {
        auto fn = loadFn(expected, TS_FORCELOADING);
        EXPECT_CALL(*_partition, load(_, _)).WillOnce(Invoke(std::move(fn)));
        table->forceLoad(target);
        EXPECT_EQ(expected.status, table->getTableStatus());
        checkError(*table, expected.error);
    }
}

TEST_F(TableTest, testForceLoadWaitRefCount) {
    auto table = makeDeployedTable(1);
    auto target = TableMetaUtil::makeTarget(1, "/index/root", "/config/root");
    auto expected = TS_LOADED;
    auto fn = loadFn(expected, TS_FORCELOADING);

    EXPECT_CALL(*_partition, isInUse()).WillOnce(Return(true));
    EXPECT_CALL(*_partition, load(_, _)).WillOnce(Invoke(std::move(fn)));
    table->forceLoad(target);
    EXPECT_EQ(TS_UNLOADED, table->getTableStatus());
    EXPECT_CALL(*_partition, isInUse()).WillOnce(Return(false));
    table->forceLoad(target);
    EXPECT_EQ(expected, table->getTableStatus());
}

TEST_F(TableTest, testReload) {
    auto table = makeDeployedTable(1);
    auto target = TableMetaUtil::makeTarget(1, "/index/root", "/config/root");
    auto tsStatusVec = vector<StatusAndError<TableStatus>>{
        StatusAndError<TableStatus>(TS_LOADED),
        StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_UNKNOWN_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_FORCE_REOPEN_UNKNOWN_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_CONFIG, CONFIG_LOAD_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_LACK_MEM, LOAD_LACKMEM_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_LACK_MEM, LOAD_FORCE_REOPEN_LACKMEM_ERROR),
        StatusAndError<TableStatus>(TS_FORCE_RELOAD, FORCE_RELOAD_ERROR),
    };

    for (auto expected : tsStatusVec) {
        auto fn = loadFn(expected, TS_FORCELOADING);
        EXPECT_CALL(*_partition, load(_, _)).WillOnce(Invoke(std::move(fn)));
        table->reload(target);
        EXPECT_EQ(expected.status, table->getTableStatus());
        checkError(*table, expected.error);
    }
}

TEST_F(TableTest, testReLoadWaitRefCount) {
    auto table = makeDeployedTable(1);
    auto target = TableMetaUtil::makeTarget(1, "/index/root", "/config/root");
    auto expected = TS_LOADED;
    auto fn = loadFn(expected, TS_FORCELOADING);

    EXPECT_CALL(*_partition, isInUse()).WillOnce(Return(true));
    EXPECT_CALL(*_partition, load(_, _)).WillOnce(Invoke(std::move(fn)));
    table->reload(target);
    EXPECT_EQ(TS_UNLOADED, table->getTableStatus());

    EXPECT_CALL(*_partition, isInUse()).WillOnce(Return(false));
    table->reload(target);
    EXPECT_EQ(expected, table->getTableStatus());
}

TEST_F(TableTest, testPreload) {
    auto table = makeDeployedTable(1);
    auto target = TableMetaUtil::makeTarget(1, "/index/root", "/config/root");
    auto tsStatusVec = vector<StatusAndError<TableStatus>>{
        StatusAndError<TableStatus>(TS_LOADED),
        StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_UNKNOWN_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_FORCE_REOPEN_UNKNOWN_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_CONFIG, CONFIG_LOAD_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_LACK_MEM, LOAD_LACKMEM_ERROR),
        StatusAndError<TableStatus>(TS_ERROR_LACK_MEM, LOAD_FORCE_REOPEN_LACKMEM_ERROR),
        StatusAndError<TableStatus>(TS_FORCE_RELOAD, FORCE_RELOAD_ERROR),
    };

    for (auto expected : tsStatusVec) {
        auto fn = loadFn(expected, TS_PRELOADING);
        EXPECT_CALL(*_partition, load(_, _)).WillOnce(Invoke(std::move(fn)));
        table->preload(target);
        if (expected.status != TS_LOADED) {
            if (expected.status == TS_FORCE_RELOAD) {
                EXPECT_EQ(TS_PRELOAD_FORCE_RELOAD, table->getTableStatus());
            } else {
                EXPECT_EQ(TS_PRELOAD_FAILED, table->getTableStatus());
            }
        } else {
            EXPECT_EQ(TS_LOADED, table->getTableStatus());
        }
        checkError(*table, expected.error);
    }
}

#undef loadFn

TEST_F(TableTest, testUnload) {
    auto table = makeLoadedTable(1);
    auto target = TableMetaUtil::makeTarget(1, "/index/root", "/config/root");
    auto fn = [this, &table]() { checkTableStatus(*table, TS_UNLOADING); };

    EXPECT_CALL(*_partition, isInUse()).WillOnce(Return(true));
    table->unload();
    EXPECT_EQ(TS_LOADED, table->getTableStatus());

    EXPECT_CALL(*_partition, isInUse()).WillOnce(Return(false));
    EXPECT_CALL(*_partition, unload()).WillOnce(Invoke(std::move(fn)));
    table->unload();
    EXPECT_EQ(TS_UNLOADED, table->getTableStatus());
}

void TableTest::doTestRoleSwitch(RoleType currentRole, RoleType targetRole, bool allowFollowerWrite) {
    auto table = makeLoadedTable(1);
    table->_partitionMeta->setRoleType(currentRole);
    table->_partitionMeta->setTableMode(TM_READ_WRITE);
    table->setAllowFollowerWrite(allowFollowerWrite);
    auto target = TableMetaUtil::makeTarget(1, "/index/root", "/config/root");
    target.setTableMode(TM_READ_WRITE);
    target.setRoleType(targetRole);

    auto writer = std::make_shared<TableWriter>();
    std::shared_ptr<TableWriter> nullWriter;

    // create new partition failed
    EXPECT_CALL(*_partition, getTableWriter()).WillOnce(Return(writer));
    EXPECT_CALL(*_partition, stopWrite(SuezPartition::SWO_DRC)).Times(1);
    EXPECT_CALL(*_partition, setTableWriter(nullWriter));
    EXPECT_CALL(*_factory, create(_, _, _)).WillOnce(Return(nullptr));
    table->doRoleSwitch(target);
    ASSERT_EQ(allowFollowerWrite || RT_LEADER == targetRole, writer->_enableWrite);
    ASSERT_EQ(TS_ROLE_SWITCH_ERROR, table->getTableStatus());
    ASSERT_EQ(_partition, table->_suezPartition.get());
    ASSERT_EQ(currentRole, table->_partitionMeta->getRoleType());

    // new partition load failed
    EXPECT_CALL(*_partition, getTableWriter()).WillOnce(Return(writer));
    EXPECT_CALL(*_partition, setTableWriter(nullWriter));
    EXPECT_CALL(*_partition, stopWrite(SuezPartition::SWO_DRC)).Times(1);
    auto newPart =
        std::make_shared<NiceMockSuezPartition>(std::make_shared<CurrentPartitionMeta>(), table->getPid(), nullptr);
    EXPECT_CALL(*(newPart), isRecovered()).WillRepeatedly(Return(true));
    EXPECT_CALL(*_factory, create(_, _, _)).WillOnce(Return(newPart));
    EXPECT_CALL(*newPart, setTableWriter(writer));
    EXPECT_CALL(*newPart, load(_, false)).WillOnce(Return(StatusAndError<TableStatus>(TS_ERROR_LACK_MEM)));
    EXPECT_CALL(*newPart, stopWrite(SuezPartition::SWO_ALL)).Times(1);
    EXPECT_CALL(*_partition, isInUse()).Times(0);
    table->doRoleSwitch(target);
    ASSERT_FALSE(table->_switchState);
    ASSERT_EQ(allowFollowerWrite || RT_LEADER == targetRole, writer->_enableWrite);
    ASSERT_EQ(_partition, table->_suezPartition.get());
    ASSERT_EQ(TS_ROLE_SWITCH_ERROR, table->getTableStatus());
    ASSERT_EQ(currentRole, table->_partitionMeta->getRoleType());

    // load success
    EXPECT_CALL(*_partition, getTableWriter()).WillOnce(Return(writer));
    EXPECT_CALL(*_partition, setTableWriter(nullWriter));
    EXPECT_CALL(*_partition, stopWrite(SuezPartition::SWO_DRC)).Times(1);
    EXPECT_CALL(*_factory, create(_, _, _)).WillOnce(Return(newPart));
    std::atomic<bool> loadedFlag{false};
    auto loadFn = [&loadedFlag]() {
        loadedFlag.store(true);
        return StatusAndError<TableStatus>(TS_LOADED);
    };
    EXPECT_CALL(*newPart, setTableWriter(writer));
    EXPECT_CALL(*newPart, load(_, false)).WillOnce(Invoke(std::move(loadFn)));
    std::atomic<bool> inuseFlag{true};
    auto isInUseFn = [&inuseFlag]() { return inuseFlag.load(); };
    EXPECT_CALL(*_partition, isInUse()).WillRepeatedly(Invoke(std::move(isInUseFn)));
    EXPECT_CALL(*_partition, unload()).Times(1);

    // run in switch thread
    std::thread thread([&]() { table->doRoleSwitch(target); });
    while (!loadedFlag) {
        // wait new partition loaded
        usleep(5000); // 5ms
    }

    // check state in TS_ROLE_SWITCHING
    ASSERT_EQ(TS_ROLE_SWITCHING, table->_partitionMeta->getTableStatus());
    ASSERT_TRUE(table->_switchState);
    ASSERT_EQ(_partition, table->_suezPartition.get());
    ASSERT_EQ(newPart.get(), table->getSuezPartition().get());
    auto writer1 = std::make_shared<TableWriter>();
    auto writer2 = std::make_shared<TableWriter>();
    EXPECT_CALL(*newPart, getTableWriter()).WillOnce(Return(nullptr));
    EXPECT_CALL(*_partition, getTableWriter()).WillOnce(Return(writer2));
    ASSERT_EQ(writer2.get(), table->getTableWriter().get());
    EXPECT_CALL(*newPart, getTableWriter()).WillOnce(Return(writer1));
    EXPECT_CALL(*_partition, getTableWriter()).Times(0);
    ASSERT_EQ(writer1.get(), table->getTableWriter().get());

    inuseFlag.store(false);
    thread.join();
    ASSERT_EQ(newPart.get(), table->_suezPartition.get());
    ASSERT_EQ(targetRole, table->_partitionMeta->getRoleType());
    ASSERT_EQ(TS_LOADED, table->getTableStatus());
    ASSERT_FALSE(table->_switchState);
    EXPECT_CALL(*newPart, getTableWriter()).WillOnce(Return(writer1));
    ASSERT_EQ(writer1.get(), table->getTableWriter().get());
}

TEST_F(TableTest, testBecomeLeader) { doTestRoleSwitch(RT_FOLLOWER, RT_LEADER); }

TEST_F(TableTest, testNoLongerLeader) { doTestRoleSwitch(RT_LEADER, RT_FOLLOWER); }

TEST_F(TableTest, testNoLongerLeaderAllowsFollowerWrite) { doTestRoleSwitch(RT_LEADER, RT_FOLLOWER, true); }

TEST_F(TableTest, testCleanIncVersion) {
    auto table = makeLoadedTable(1);
    ASSERT_EQ(DS_DEPLOYDONE, table->getTableDeployStatus(1));

    table->_partitionMeta->setDeployStatus(2, DS_DEPLOYDONE);
    table->_partitionMeta->setIncVersion(2);
    std::set<IncVersion> keeps{2};

    table->cleanIncVersion(keeps);
    ASSERT_EQ(DS_UNKNOWN, table->getTableDeployStatus(1));
}

TEST_F(TableTest, testFinalTargetToTarget) {
    auto table = makeLoadedTable(1);
    table->finalTargetToTarget();
    ASSERT_EQ(TS_LOADED, table->getTableStatus());

    table->setTableStatus(TS_PRELOADING);
    table->finalTargetToTarget();
    ASSERT_EQ(TS_LOADING, table->getTableStatus());

    table->setTableStatus(TS_PRELOAD_FAILED);
    table->finalTargetToTarget();
    ASSERT_EQ(TS_ERROR_UNKNOWN, table->getTableStatus());

    table->setTableStatus(TS_PRELOAD_FORCE_RELOAD);
    table->finalTargetToTarget();
    ASSERT_EQ(TS_FORCE_RELOAD, table->getTableStatus());
}

TEST_F(TableTest, testTargetReachedNeedDeploy) {
    auto pid = TableMetaUtil::makePid("table1");
    auto current = TableMetaUtil::make(0);
    auto target = TableMetaUtil::make(0);
    auto table = TableTestUtil::make(pid, current);
    EXPECT_FALSE(table->targetReached(target, true));
}

TEST_F(TableTest, testTargetReachedDefault) {
    auto notLoadedTs = {TS_UNLOADED,
                        TS_LOADING,
                        TS_UNLOADING,
                        TS_FORCELOADING,
                        TS_FORCE_RELOAD,
                        TS_PRELOADING,
                        TS_PRELOAD_FAILED,
                        TS_PRELOAD_FORCE_RELOAD,
                        TS_ERROR_LACK_MEM,
                        TS_ERROR_CONFIG,
                        TS_ERROR_UNKNOWN};
    auto pid = TableMetaUtil::makePid("table1");

    {
        // deploy done, it becomes ready.
        auto current = TableMetaUtil::make(0, TS_LOADED, DS_DEPLOYDONE);
        auto target = TableMetaUtil::make(0);
        auto table = TableTestUtil::make(pid, current);
        EXPECT_TRUE(table->targetReached(target, true));
        for (auto ts : notLoadedTs) {
            table->setTableStatus(ts);
            EXPECT_EQ(false, table->targetReached(target, true));
        }
    }
}

TEST_F(TableTest, testReachedTargetRawFile) {
    auto notLoadedTs = {TS_UNLOADED,
                        TS_LOADING,
                        TS_UNLOADING,
                        TS_FORCELOADING,
                        TS_FORCE_RELOAD,
                        TS_PRELOADING,
                        TS_PRELOAD_FAILED,
                        TS_PRELOAD_FORCE_RELOAD,
                        TS_ERROR_LACK_MEM,
                        TS_ERROR_CONFIG,
                        TS_ERROR_UNKNOWN};
    auto pid = TableMetaUtil::makePid("table1");
    auto current = TableMetaUtil::makeWithTableType(0, TS_LOADED, DS_DEPLOYDONE, SPT_RAWFILE);
    auto target = TableMetaUtil::make(0);
    auto table = TableTestUtil::make(pid, current);
    EXPECT_TRUE(table->targetReached(target, true));

    for (auto ts : notLoadedTs) {
        table->setTableStatus(ts);
        EXPECT_FALSE(table->targetReached(target, true));
    }
}

TEST_F(TableTest, testReachedTargetSptIndexlib) {
    auto pid = TableMetaUtil::makePid("table1");
    // indexlib type, loaded and preloading is ready
    auto current = TableMetaUtil::makeWithTableType(0, TS_LOADED, DS_DEPLOYDONE, SPT_INDEXLIB, true);
    auto target = TableMetaUtil::make(0);
    auto table = TableTestUtil::make(pid, current);

    EXPECT_TRUE(table->targetReached(target, true));

    table->setTableStatus(TS_PRELOADING);
    EXPECT_TRUE(table->targetReached(target, true));

    table->setTableStatus(TS_PRELOAD_FAILED);
    EXPECT_TRUE(table->targetReached(target, true));

    table->setTableStatus(TS_PRELOAD_FORCE_RELOAD);
    EXPECT_TRUE(table->targetReached(target, true));

    auto noReadyTs = {TS_UNLOADED,
                      TS_LOADING,
                      TS_UNLOADING,
                      TS_FORCELOADING,
                      TS_FORCE_RELOAD,
                      TS_ERROR_LACK_MEM,
                      TS_ERROR_CONFIG,
                      TS_ERROR_UNKNOWN};
    for (auto ts : noReadyTs) {
        table->setTableStatus(ts);
        EXPECT_FALSE(table->targetReached(target, true));
    }

    {
        // check full index loaded
        auto current = TableMetaUtil::makeWithTableType(0, TS_LOADED, DS_DEPLOYDONE, SPT_INDEXLIB, false);
        auto target = TableMetaUtil::make(0);
        auto table = TableTestUtil::make(pid, current);
        EXPECT_FALSE(table->targetReached(target, true));
    }
}

TEST_F(TableTest, testReachedTargetSptIndexlibConfigPathChange) {
    auto pid = TableMetaUtil::makePid("table1");
    // indexlib type, loaded and preloading is ready
    auto current = TableMetaUtil::makeWithTableType(0, TS_LOADED, DS_DEPLOYDONE, SPT_INDEXLIB, true);
    auto target = TableMetaUtil::make(0);
    target.setConfigPath("newConfigPath");
    auto table = TableTestUtil::make(pid, current);

    EXPECT_FALSE(table->targetReached(target, true));
}

TEST_F(TableTest, testReachedTargetSptIndexlibIndexRootChange) {
    auto pid = TableMetaUtil::makePid("table1");
    // indexlib type, loaded and preloading is ready
    auto current = TableMetaUtil::makeWithTableType(0, TS_LOADED, DS_DEPLOYDONE, SPT_INDEXLIB, true);
    auto target = TableMetaUtil::make(0);
    target.setIndexRoot("newIndexRoot");
    current.setLoadedIndexRoot("oldIndexRoot");
    auto table = TableTestUtil::make(pid, current);

    EXPECT_FALSE(table->targetReached(target, true));
}

TEST_F(TableTest, testTargetReachedIsRecovered) {
    auto table = makeLoadedTable(1);
    auto target = TableMetaUtil::makeTarget(1);

    EXPECT_FALSE(table->targetReached(target, true));
    EXPECT_CALL(*_partition, isRecovered()).WillRepeatedly(Return(true));
    EXPECT_TRUE(table->targetReached(target, true));
}

TEST_F(TableTest, testMaybeWaitOnRoleSwitch) {
    auto table = makeLoadedTable(1);
    table->setTableStatus(TS_ROLE_SWITCHING);

    auto target = TableMetaUtil::makeTarget(1, "/index/root", "/config/root");
    // deploy
    EXPECT_CALL(*_partition, deploy(_, _)).Times(0);
    table->deploy(target, false);

    // cancelDeploy
    EXPECT_CALL(*_partition, cancelDeploy()).Times(0);
    table->cancelDeploy();

    // cancelLoad
    EXPECT_CALL(*_partition, cancelLoad()).Times(0);
    table->cancelLoad();

    // load
    EXPECT_CALL(*_partition, load(_, _)).Times(0);
    table->load(target);

    // forceLoad
    EXPECT_CALL(*_partition, isInUse()).Times(0);
    EXPECT_CALL(*_partition, load(_, _)).Times(0);
    table->forceLoad(target);

    // reload
    EXPECT_CALL(*_partition, isInUse()).Times(0);
    EXPECT_CALL(*_partition, unload()).Times(0);
    EXPECT_CALL(*_partition, load(_, _)).Times(0);
    table->reload(target);

    // preload
    EXPECT_CALL(*_partition, load(_, _)).Times(0);
    table->preload(target);

    // unload
    EXPECT_CALL(*_partition, isInUse()).Times(0);
    EXPECT_CALL(*_partition, unload()).Times(0);
    table->unload();

    // can not commit when role switching
    EXPECT_FALSE(table->needCommit());
}

} // namespace suez
