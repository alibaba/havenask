#include "suez/table/TableManager.h"

#include "FakeAsyncTodoRunner.h"
#include "FakeSuezPartitionFactory.h"
#include "TableTestUtil.h"
#include "autil/EnvUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/testlib/mock_index_partition.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"
#include "suez/common/TablePathDefine.h"
#include "suez/common/TargetLayout.h"
#include "suez/common/test/TableMetaUtil.h"
#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/TableReader.h"
#include "suez/table/IndexPartitionAdapter.h"
#include "suez/table/TodoListExecutor.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace autil::legacy::json;
using namespace indexlib::partition;
using namespace indexlib::testlib;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TableManagerTest);

class TableManagerTest : public TESTBASE {
public:
    void setUp() override {
        alog::Logger::getRootLogger()->setLevel(alog::LOG_LEVEL_WARN);

        _dpThreadPool.reset(new autil::ThreadPool(20, 20));
        _loadThreadPool.reset(new autil::ThreadPool(10, 10));
        _dpThreadPool->start("SuezDp");
        _loadThreadPool->start("SuezLoad");

        _initParam.loadThreadPool = _loadThreadPool.get();
        _initParam.deployThreadPool = _dpThreadPool.get();
        _factory = std::make_shared<FakeSuezPartitionFactory>();
        _schema = IndexlibPartitionCreator::CreateSchema(
            "test_table", "id:int64;buyer_age:uint32;attr_str:string;", "id:primarykey64:id", "buyer_age;attr_str", "");
    }

protected:
    HeartbeatTarget prepareTarget(const std::vector<std::pair<string, string>> &strVec,
                                  bool cleanDisk = false,
                                  bool allowPreload = false) const {
        TableMetas metas;
        string signatureStr = "";
        for (const auto &it : strVec) {
            auto meta = TableMetaUtil::makeTableMeta(it.first, it.second);
            metas[meta.first] = meta.second;
            signatureStr += it.first + ";" + it.second;
        }
        HeartbeatTarget target;
        target.setTableMetas(metas);
        target._cleanDisk = cleanDisk;
        target._preload = allowPreload;
        target._signature =
            StringUtil::toString(HashAlgorithm::hashString64(signatureStr.c_str(), signatureStr.size()));
        return target;
    }

    void checkTable(const TablePtr &table,
                    TableStatus expectedTs,
                    IncVersion incVersion = -1,
                    DeployStatus expectedDs = DS_UNKNOWN) {
        ASSERT_EQ(expectedTs, table->getTableStatus()) << FastToJsonString(table->getPid());
        if (incVersion >= 0) {
            ASSERT_EQ(expectedDs, table->getTableDeployStatus(incVersion))
                << FastToJsonString(table->getPid()) << incVersion;
        }
    }

    std::shared_ptr<TableManager> makeInitedTM(bool useFakeAsyncRunner = false) {
        auto tm = std::make_shared<TableManager>();
        tm->_partitionFactory.reset(new FakeSuezPartitionFactory);
        EXPECT_TRUE(tm->init(_initParam));
        if (useFakeAsyncRunner) {
            initFakeAsyncTodoRunner(tm);
        }
        tm->_scheduleConfig.allowPreload = true;
        return tm;
    }

    // FakeAsyncTodoRunner 可以手动控制异步执行何时结束
    void initFakeAsyncTodoRunner(const std::shared_ptr<TableManager> &tableManager) {
        tableManager->_todoListExecutor = std::make_unique<TodoListExecutor>();
        tableManager->_todoListExecutor->addRunner(
            std::make_unique<FakeAsyncTodoRunner>(_initParam.loadThreadPool, "load"),
            {OP_LOAD,
             OP_PRELOAD,
             OP_FORCELOAD,
             OP_RELOAD,
             OP_UNLOAD,
             OP_BECOME_LEADER,
             OP_NO_LONGER_LEADER,
             OP_COMMIT});
        tableManager->_todoListExecutor->addRunner(
            std::make_unique<FakeAsyncTodoRunner>(_initParam.deployThreadPool, "deploy"),
            {OP_INIT, OP_DEPLOY, OP_DIST_DEPLOY, OP_CLEAN_DISK, OP_CLEAN_INC_VERSION});
    }

    TodoRunner *getTodoRunner(const std::shared_ptr<TableManager> &tableManager, const std::string &name) {
        if (!tableManager->_todoListExecutor) {
            return nullptr;
        }
        for (const auto &runner : tableManager->_todoListExecutor->_runners) {
            if (runner->getName() == name) {
                return runner.get();
            }
        }
        return nullptr;
    }

    indexlib::index_base::Version makeIndexVersion(IncVersion version) {
        indexlib::index_base::Version v;
        v.SetVersionId(version);
        return v;
    }

    std::unique_ptr<IndexPartitionAdapter> makeIndexPartitionAdapterWithVersion(const PartitionId &pid,
                                                                                IncVersion version) {
        auto indexPart = MockIndexPartition::MakeNice();
        IndexPartitionPtr partPtr(indexPart);
        auto reader = new NiceMockIndexPartitionReader(_schema);
        IndexPartitionReaderPtr readerPtr(reader);
        auto indexPartAdapter = std::make_unique<IndexPartitionAdapter>("", "", partPtr);

        EXPECT_CALL(*indexPart, Open(_, _, _, _, _)).WillOnce(Return(IndexPartition::OS_OK));
        EXPECT_CALL(*indexPart, GetReader()).WillRepeatedly(Return(readerPtr));

        EXPECT_CALL(*reader, GetVersion()).WillOnce(Return(makeIndexVersion(version)));
        EXPECT_CALL(*reader, GetIndexAccessCounters()).WillRepeatedly(ReturnRef(_counters));
        EXPECT_CALL(*reader, GetAttributeAccessCounters()).WillRepeatedly(ReturnRef(_counters));

        _readerMap[pid] = reader;
        _indexMap[pid] = indexPart;

        return indexPartAdapter;
    }

    void updateTargetReached(TableManager *tm, const HeartbeatTarget &target) {
        // init
        auto ret = tm->update(target, target, false, true);
        ASSERT_EQ(UR_NEED_RETRY, ret);

        std::vector<TablePtr> tables;
        for (const auto &meta : target.getTableMetas()) {
            const auto &pid = meta.first;
            const auto &tableMeta = meta.second;
            auto table = tm->getTable(pid);
            tables.push_back(table);
            checkTable(table, TS_UNLOADED, tableMeta.incVersion);
            if (tableMeta.getTableType() == SPT_INDEXLIB) {
                auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
                ASSERT_TRUE(part != nullptr);
                // dp
                EXPECT_CALL(*(part), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
                EXPECT_CALL(*(part), doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
                // load
                EXPECT_CALL(*(part), loadTableConfig(_, _)).WillRepeatedly(Return(true));
                auto indexPartAdapter = makeIndexPartitionAdapterWithVersion(pid, tableMeta.incVersion);
                EXPECT_CALL(*part, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter))));
                // rt recover
                EXPECT_CALL(*(part), isRecovered()).WillRepeatedly(Return(true));
            } else {
                auto part = dynamic_pointer_cast<NiceMockSuezRawFilePartition>(table->_suezPartition);
                ASSERT_TRUE(part != nullptr);

                // dp
                EXPECT_CALL(*(part), doDeploy(_, _)).WillOnce(Return(DS_DEPLOYDONE));
                // load
                EXPECT_CALL(*(part), doLoad(_)).WillOnce(Return(TS_LOADED));
                // rt recover
                EXPECT_CALL(*(part), isRecovered()).WillRepeatedly(Return(true));
            }
        }

        ret = tm->update(target, target, false, true);
        ASSERT_EQ(UR_NEED_RETRY, ret);

        ret = tm->update(target, target, false, true);
        ASSERT_EQ(UR_NEED_RETRY, ret);
        for (auto table : tables) {
            checkTable(table, TS_LOADED);
        }

        ret = tm->update(target, target, false, true);
        ASSERT_EQ(UR_REACH_TARGET, ret);
    }

    void checkInUseVersions(const set<IncVersion> &expectedVersions, const CleanIncVersion *cleanIncVersion) {
        auto &caculatedVersions = cleanIncVersion->_inUseIncVersions;
        ASSERT_EQ(expectedVersions.size(), caculatedVersions.size());
        for (const auto &version : expectedVersions) {
            ASSERT_EQ(1, caculatedVersions.count(version));
        }
    }

protected:
    InitParam _initParam;
    std::unique_ptr<autil::ThreadPool> _dpThreadPool;
    std::unique_ptr<autil::ThreadPool> _loadThreadPool;
    std::shared_ptr<FakeSuezPartitionFactory> _factory;
    indexlib::config::IndexPartitionSchemaPtr _schema;
    IndexPartitionReader::AccessCounterMap _counters;
    std::map<PartitionId, NiceMockIndexPartitionReader *> _readerMap;
    std::map<PartitionId, NiceMockIndexPartition *> _indexMap;
};

TEST_F(TableManagerTest, testConstruct) {
    TableManager tm;
    EXPECT_TRUE(tm._decisionMaker.get() != nullptr);
    EXPECT_TRUE(tm._partitionFactory.get() != nullptr);
    EXPECT_TRUE(tm._scheduleConfig.allowForceLoad);
    EXPECT_FALSE(tm._scheduleConfig.allowReloadByConfig);
    EXPECT_FALSE(tm._scheduleConfig.allowReloadByIndexRoot);
}

TEST_F(TableManagerTest, testInit) {
    EnvGuard guard1("RS_ALLOW_RELOAD_BY_INDEX_ROOT", "true");
    EnvGuard guard2("RS_ALLOW_FORCELOAD", "false");
    TableManager tm;
    ASSERT_TRUE(tm.init(_initParam));
    auto executor = tm._todoListExecutor.get();
    EXPECT_TRUE(executor != nullptr);
    EXPECT_TRUE(executor->getRunner(OP_COMMIT) != nullptr);
    EXPECT_TRUE(executor->getRunner(OP_COMMIT) == executor->getRunner(OP_LOAD));
    EXPECT_FALSE(tm._scheduleConfig.allowForceLoad);
    EXPECT_FALSE(tm._scheduleConfig.allowReloadByConfig);
    EXPECT_TRUE(tm._scheduleConfig.allowReloadByIndexRoot);
}

TEST_F(TableManagerTest, testInitBlockCache) {
    EnvGuard env1("RS_ALLOW_MEMORY", "10485760"); // 10 * 1024 * 1024
    EnvGuard env2("RS_BLOCK_CACHE", "cache_size=3;");
    TableManager tm;
    ASSERT_TRUE(tm.init(_initParam));
    EXPECT_EQ(3 * 1024 * 1024,
              tm._globalIndexResource->GetFileBlockCacheContainer()
                  ->GetAvailableFileCache("")
                  ->GetResourceInfo()
                  .maxMemoryUse);
}

TEST_F(TableManagerTest, testInitSearchCacheSucc) {
    EnvGuard env1("RS_ALLOW_MEMORY", "10485760");      // 10 * 1024 * 1024
    EnvGuard env2("RS_SEARCH_CACHE", "cache_size=7;"); // 7MB
    TableManager tm;
    ASSERT_TRUE(tm.init(_initParam));
    EXPECT_EQ(7 * 1024 * 1024, tm._globalIndexResource->GetSearchCache()->GetCacheSize());
}

TEST_F(TableManagerTest, testInitSearchCacheFailed) {
    EnvGuard env("RS_ALLOW_MEMORY", "10485760"); // 10 * 1024 * 1024
    TableManager tm;
    ASSERT_TRUE(tm.init(_initParam));
    EXPECT_FALSE(tm._globalIndexResource->GetSearchCache());
}

TEST_F(TableManagerTest, testGetOrCreateTable) {
    TableManager tm;
    auto pid = TableMetaUtil::makePid("t1");
    auto target = TableMetaUtil::make(1);

    ASSERT_FALSE(tm.getTable(pid));
    ASSERT_TRUE(tm.getOrCreateTable(pid, target));
    ASSERT_TRUE(tm.getTable(pid));
    ASSERT_EQ(1, tm._tableMap.size());

    pid = TableMetaUtil::makePid("t2");
    ASSERT_FALSE(tm.getTable(pid));
    ASSERT_TRUE(tm.getOrCreateTable(pid, target));
    ASSERT_TRUE(tm.getTable(pid));
    ASSERT_EQ(2, tm._tableMap.size());

    pid = TableMetaUtil::makePid("t3");
    target = TableMetaUtil::make(1, TS_UNKNOWN, DS_UNKNOWN, "", "");
    ASSERT_FALSE(tm.getTable(pid));
    ASSERT_TRUE(tm.getOrCreateTable(pid, target));
    ASSERT_TRUE(tm.getTable(pid));
    ASSERT_EQ(3, tm._tableMap.size());
}

TEST_F(TableManagerTest, testUpdateEmptyTarget) {
    TableManager tm;
    tm.init(_initParam);

    HeartbeatTarget target, finalTarget;
    auto ret = tm.update(target, finalTarget, false, false);

    ASSERT_EQ(UR_REACH_TARGET, ret);
}

TEST_F(TableManagerTest, testSimpleProcess) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config_/t1/index"}, {"t2", "2_/t2/config_/t2/index"}});

    // init
    auto ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);

    auto pid1 = TableMetaUtil::makePid("t1");
    auto table1 = tm->getTable(pid1);
    ASSERT_TRUE(table1);
    auto pid2 = TableMetaUtil::makePid("t2");
    auto table2 = tm->getTable(pid2);
    ASSERT_TRUE(table2);
    checkTable(table1, TS_UNLOADED, 1);
    checkTable(table2, TS_UNLOADED, 2);

    auto part1 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table1->_suezPartition);
    ASSERT_TRUE(part1 != nullptr);
    auto part2 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table2->_suezPartition);
    ASSERT_TRUE(part2 != nullptr);

    // dp
    EXPECT_CALL(*(part1), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part2), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part1), doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part2), doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));

    ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table1, TS_UNLOADED, 1, DS_DEPLOYDONE);
    checkTable(table2, TS_UNLOADED, 2, DS_DEPLOYDONE);
    EXPECT_EQ("/t1/config", table1->_partitionMeta->getConfigPath());
    EXPECT_EQ("/t1/index", table1->_partitionMeta->getIndexRoot());
    EXPECT_EQ("/t2/config", table2->_partitionMeta->getConfigPath());
    EXPECT_EQ("/t2/index", table2->_partitionMeta->getIndexRoot());

    // load
    EXPECT_CALL(*(part1), loadTableConfig(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*(part2), loadTableConfig(_, _)).WillOnce(Return(true));

    auto indexPartAdapter1 = makeIndexPartitionAdapterWithVersion(pid1, 1);
    auto indexPartAdapter2 = makeIndexPartitionAdapterWithVersion(pid2, 2);
    EXPECT_CALL(*part1, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter1))));
    EXPECT_CALL(*part2, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter2))));

    ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table1, TS_LOADED, 1, DS_DEPLOYDONE);
    checkTable(table2, TS_LOADED, 2, DS_DEPLOYDONE);
    EXPECT_TRUE(table1->_partitionMeta->getFullIndexLoaded());
    EXPECT_EQ("/t1/config", table1->_partitionMeta->getLoadedConfigPath());
    EXPECT_EQ("/t1/index", table1->_partitionMeta->getLoadedIndexRoot());
    EXPECT_TRUE(table2->_partitionMeta->getFullIndexLoaded());
    EXPECT_EQ("/t2/config", table2->_partitionMeta->getLoadedConfigPath());
    EXPECT_EQ("/t2/index", table2->_partitionMeta->getLoadedIndexRoot());
    EXPECT_EQ(1, table1->_partitionMeta->getIncVersion());
    EXPECT_EQ(2, table2->_partitionMeta->getIncVersion());

    // finish
    EXPECT_CALL(*part1, isRecovered()).WillRepeatedly(Return(true));
    EXPECT_CALL(*part2, isRecovered()).WillRepeatedly(Return(true));
    ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_REACH_TARGET, ret);
}

TEST_F(TableManagerTest, testDeploy) {
    auto tm = makeInitedTM();
    auto tmpPath = GET_TEST_DATA_PATH() + "/table_test/testDeploy_temp_dir/";
    auto indexRoot = tmpPath + "runtimedata/";
    auto configPath = tmpPath + "zone_config/";
    tm->_localIndexRoot = indexRoot;
    tm->_localTableConfigBaseDir = configPath;
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(indexRoot + "tableNeedClean", true));
    ASSERT_EQ(fslib::EC_TRUE, fslib::fs::FileSystem::isExist(indexRoot + "tableNeedClean"));
    auto target = prepareTarget({{"t1", "1_/t1/config_/t1/index"}});

    // init
    auto ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);

    auto table = tm->getTable(TableMetaUtil::makePid("t1"));
    ASSERT_TRUE(table != nullptr);
    checkTable(table, TS_UNLOADED, 1);
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);

    // dp config failed
    EXPECT_CALL(*(part), doDeployConfig(_, _)).WillOnce(Return(DS_FAILED));
    ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table, TS_UNLOADED, 1, DS_FAILED);

    // dp config done dp index failed
    EXPECT_CALL(*(part), doDeployConfig(_, _)).WillRepeatedly(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part), doDeployIndex(_, false)).WillOnce(Return(DS_FAILED));

    ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table, TS_UNLOADED, 1, DS_FAILED);

    // retry dp index and dp index canceled
    EXPECT_CALL(*(part), doDeployIndex(_, false)).WillOnce(Return(DS_CANCELLED));
    ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table, TS_UNLOADED, 1, DS_CANCELLED);

    // lacking disk quota, clean unsed tables and retry dp
    EXPECT_CALL(*part, doDeployIndex(_, false)).WillOnce(Return(DS_DISKQUOTA));
    ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table, TS_UNLOADED, 1, DS_DISKQUOTA);

    EXPECT_CALL(*(part), doDeployIndex(_, false)).WillOnce(Return(DS_DEPLOYDONE));
    ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table, TS_UNLOADED, 1, DS_DEPLOYDONE);
    ASSERT_EQ("/t1/config", table->_partitionMeta->getConfigPath());
    ASSERT_EQ("/t1/index", table->_partitionMeta->getIndexRoot());

    // dist deploy
    auto target2 = prepareTarget({{"t1", "1_/t1/config2_/t1/index2"}});
    EXPECT_CALL(*(part), doDeployConfig(_, _)).Times(0);
    EXPECT_CALL(*(part), doDeployIndex(_, _)).Times(0);
    ret = tm->update(target2, target2, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table, TS_ERROR_CONFIG, 1, DS_DEPLOYDONE);

    tm->_scheduleConfig.allowReloadByConfig = true;
    EXPECT_CALL(*(part), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part), doDeployIndex(_, true)).WillOnce(Return(DS_DEPLOYDONE));
    ret = tm->update(target2, target2, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table, TS_ERROR_CONFIG, 1, DS_DEPLOYDONE);
}

TEST_F(TableManagerTest, testUpdateIncVersion) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config_/t1/index"}, {"t2", "2_/t2/config_/t2/index"}});
    tm->_scheduleConfig.allowFastCleanIncVersion = false;

    // 1. make target loaded
    updateTargetReached(tm.get(), target);

    auto table1 = tm->getTable(TableMetaUtil::makePid("t1"));
    ASSERT_TRUE(table1);
    auto table2 = tm->getTable(TableMetaUtil::makePid("t2"));
    ASSERT_TRUE(table2);

    auto part1 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table1->_suezPartition);
    ASSERT_TRUE(part1 != nullptr);
    auto part2 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table2->_suezPartition);
    ASSERT_TRUE(part2 != nullptr);

    // 2. normal inc version
    auto target2 = prepareTarget({{"t1", "2_/t1/config_/t1/index"}, {"t2", "2_/t2/config_/t2/index"}});
    EXPECT_CALL(*part1, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part1, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part2, doDeployConfig(_, _)).Times(0);
    EXPECT_CALL(*part2, doDeployIndex(_, _)).Times(0);
    auto ret = tm->update(target2, target2, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table1, TS_LOADED, 2, DS_DEPLOYDONE);
    ASSERT_EQ(1, table1->_partitionMeta->getIncVersion());
    checkTable(table2, TS_LOADED, 2, DS_DEPLOYDONE);
    ASSERT_EQ(2, table2->_partitionMeta->getIncVersion());

    auto pid = TableMetaUtil::makePid("t1");
    ASSERT_TRUE(_indexMap.count(pid) == 1);
    auto indexPartition = _indexMap[pid];
    EXPECT_CALL(*indexPartition, ReOpen(false, 2)).WillOnce(Return(IndexPartition::OS_OK));
    ASSERT_TRUE(_readerMap.count(pid) == 1);
    auto reader = _readerMap[pid];

    EXPECT_CALL(*reader, GetVersion()).WillOnce(Return(makeIndexVersion(2)));

    ret = tm->update(target2, target2, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    ASSERT_EQ(2, table1->_partitionMeta->getIncVersion());
    ASSERT_EQ(2, table2->_partitionMeta->getIncVersion());

    ret = tm->update(target2, target2, false, true);
    ASSERT_EQ(UR_REACH_TARGET, ret);

    // 3. test lack memory to force reopen
    auto target3 = prepareTarget({{"t1", "3_/t1/config_/t1/index"}, {"t2", "2_/t2/config_/t2/index"}});
    EXPECT_CALL(*part1, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part1, doDeployIndex(_, false)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part2, doDeployConfig(_, _)).Times(0);
    EXPECT_CALL(*part2, doDeployIndex(_, false)).Times(0);
    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table1, TS_LOADED, 2, DS_DEPLOYDONE);
    ASSERT_EQ(2, table1->_partitionMeta->getIncVersion());
    checkTable(table2, TS_LOADED, 2, DS_DEPLOYDONE);
    ASSERT_EQ(2, table2->_partitionMeta->getIncVersion());

    EXPECT_CALL(*indexPartition, ReOpen(false, 3)).WillOnce(Return(IndexPartition::OS_LACK_OF_MEMORY));
    EXPECT_CALL(*reader, GetVersion()).Times(0);
    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    ASSERT_EQ(2, table1->_partitionMeta->getIncVersion());
    checkTable(table1, TS_ERROR_LACK_MEM, 3, DS_DEPLOYDONE);

    EXPECT_CALL(*indexPartition, ReOpen(true, 3)).WillOnce(Return(IndexPartition::OS_OK));
    EXPECT_CALL(*reader, GetVersion()).WillOnce(Return(makeIndexVersion(3)));

    part1.reset(); // release ref count
    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_ERROR, ret);
    ASSERT_EQ(3, table1->_partitionMeta->getIncVersion());
    checkTable(table1, TS_LOADED, 3, DS_DEPLOYDONE);

    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_REACH_TARGET, ret);

    // 4. reload because of config path changed
    auto target4 = prepareTarget({{"t1", "3_/t1/config2_/t1/index"}, {"t2", "2_/t2/config_/t2/index"}});
    part1 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table1->_suezPartition);
    EXPECT_CALL(*part1, doDeployConfig(_, _)).Times(0);
    EXPECT_CALL(*part1, doDeployIndex(_, _)).Times(0);
    ret = tm->update(target4, target4, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    ASSERT_EQ("/t1/config", table1->_partitionMeta->getConfigPath());

    tm->_scheduleConfig.allowReloadByConfig = true;
    EXPECT_CALL(*part1, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part1, doDeployIndex(_, true)).WillOnce(Return(DS_DEPLOYDONE));
    ret = tm->update(target4, target4, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    ASSERT_EQ("/t1/config2", table1->_partitionMeta->getConfigPath());

    auto indexPartAdapter = makeIndexPartitionAdapterWithVersion(pid, 3);
    EXPECT_CALL(*part1, loadTableConfig(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*part1, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter))));

    part1.reset();
    ret = tm->update(target4, target4, false, true);
    ASSERT_EQ(UR_ERROR, ret);
    checkTable(table1, TS_LOADED, 3, DS_DEPLOYDONE);

    ret = tm->update(target4, target4, false, true);
    ASSERT_EQ(UR_REACH_TARGET, ret);

    // 5. inc rollback -> reload
    auto target5 = prepareTarget({{"t1", "2_/t1/config2_/t1/index"}, {"t2", "2_/t2/config_/t2/index"}});
    checkTable(table1, TS_LOADED, 1, DS_DEPLOYDONE);
    checkTable(table1, TS_LOADED, 2, DS_DEPLOYDONE);
    checkTable(table1, TS_LOADED, 3, DS_DEPLOYDONE);
    part1 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table1->_suezPartition);

    auto indexPartAdapter2 = makeIndexPartitionAdapterWithVersion(pid, 2);

    EXPECT_CALL(*part1, loadTableConfig(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*part1, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter2))));
    part1.reset();
    ret = tm->update(target5, target5, false, true);
    ASSERT_EQ(UR_ERROR, ret);
    checkTable(table1, TS_LOADED, 2, DS_DEPLOYDONE);
    checkTable(table1, TS_LOADED, 3, DS_DEPLOYDONE);

    ret = tm->update(target5, target5, false, true);
    ASSERT_EQ(UR_REACH_TARGET, ret);
}

TEST_F(TableManagerTest, testReopenFailToReload) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config_/t1/index"}});
    // 1. make target loaded
    updateTargetReached(tm.get(), target);

    auto table1 = tm->getTable(TableMetaUtil::makePid("t1"));
    ASSERT_TRUE(table1);
    auto part1 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table1->_suezPartition);
    ASSERT_TRUE(part1 != nullptr);

    // reopen -> reload (reopen returns exception)
    auto target3 = prepareTarget({{"t1", "3_/t1/config_/t1/index"}});
    EXPECT_CALL(*part1, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part1, doDeployIndex(_, false)).WillOnce(Return(DS_DEPLOYDONE));
    auto ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table1, TS_LOADED, 3, DS_DEPLOYDONE);
    ASSERT_EQ(1, table1->_partitionMeta->getIncVersion());

    auto pid = TableMetaUtil::makePid("t1");
    ASSERT_TRUE(_indexMap.count(pid) == 1);
    auto indexPartition = _indexMap[pid];
    ASSERT_TRUE(_readerMap.count(pid) == 1);
    auto reader = _readerMap[pid];

    EXPECT_CALL(*indexPartition, ReOpen(false, 3)).WillOnce(Return(IndexPartition::OS_UNKNOWN_EXCEPTION));
    EXPECT_CALL(*reader, GetVersion()).Times(0);

    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    ASSERT_EQ(1, table1->_partitionMeta->getIncVersion());
    checkTable(table1, TS_FORCE_RELOAD, 3, DS_DEPLOYDONE);

    auto indexPartAdapter = makeIndexPartitionAdapterWithVersion(pid, 3);
    EXPECT_CALL(*part1, loadTableConfig(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*part1, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter))));
    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_ERROR, ret);
    checkTable(table1, TS_LOADED, 3, DS_DEPLOYDONE);

    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_REACH_TARGET, ret);
}

TEST_F(TableManagerTest, testUpdateFullVersion) {
    // 0. init
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1_1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);
    auto table1 = tm->getTable(TableMetaUtil::makePidFromStr("t1_1"));
    checkTable(table1, TS_LOADED, 1, DS_DEPLOYDONE);

    // 1. normal full version switch
    auto target2 = prepareTarget({{"t1_2", "1_/t1/config2_/t1/index2"}});
    auto pid = TableMetaUtil::makePidFromStr("t1_2");
    target2._tableMetas[pid].setTableLoadType(TLT_UNLOAD_FIRST);

    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table1->_suezPartition);
    auto data = part->getPartitionData(); // ref count > 1
    ASSERT_TRUE(part->isInUse());

    auto ret = tm->update(target2, target2, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table1, TS_LOADED, 1, DS_DEPLOYDONE);
    auto table2 = tm->getTable(TableMetaUtil::makePidFromStr("t1_2"));
    ASSERT_TRUE(table2);
    checkTable(table2, TS_UNLOADED, 1, DS_UNKNOWN);
    table1 = tm->getTable(TableMetaUtil::makePidFromStr("t1_1"));
    ASSERT_TRUE(table1);

    auto part2 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table2->_suezPartition);
    EXPECT_CALL(*part2, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part2, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ret = tm->update(target2, target2, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table2, TS_UNLOADED, 1, DS_DEPLOYDONE);

    auto indexPartAdapter = makeIndexPartitionAdapterWithVersion(pid, 1);
    ASSERT_TRUE(part->isInUse());
    EXPECT_CALL(*part2, loadTableConfig(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*part2, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter))));

    ret = tm->update(target2, target2, false, true);

    ASSERT_EQ(UR_ERROR, ret);
    checkTable(table2, TS_UNLOADED, 1, DS_DEPLOYDONE); // can not load table2 before table1 unloaded

    data.reset();
    ASSERT_FALSE(part->isInUse());
    EXPECT_CALL(*part2, isRecovered()).WillRepeatedly(Return(true));
    ASSERT_EQ(UR_ERROR, tm->update(target2, target2, false, true));      // unload table1
    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true)); // remove table1, load table2
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target2, target2, false, true));

    // 2. new version force reload (file_io_exception)
    auto target3 = prepareTarget({{"t1_3", "1_/t1/config3_/t1/index3"}});
    auto data2 = part2->getPartitionData();
    ASSERT_TRUE(part2->isInUse());

    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table2, TS_LOADED, 1, DS_DEPLOYDONE);
    auto table3 = tm->getTable(TableMetaUtil::makePidFromStr("t1_3"));
    ASSERT_TRUE(table3);
    checkTable(table3, TS_UNLOADED, 1, DS_UNKNOWN);
    data2.reset();
    ASSERT_FALSE(part2->isInUse());

    part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table3->_suezPartition);
    EXPECT_CALL(*part, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table3, TS_UNLOADED, 1, DS_DEPLOYDONE);
    checkTable(table2, TS_UNLOADED, 1, DS_DEPLOYDONE);

    auto indexPart = MockIndexPartition::MakeNice();
    IndexPartitionPtr partPtr(indexPart);
    auto reader = new NiceMockIndexPartitionReader(_schema);
    IndexPartitionReaderPtr readerPtr(reader);
    auto indexPartAdapter2 = std::make_unique<IndexPartitionAdapter>("", "", partPtr);
    EXPECT_CALL(*indexPart, Open(_, _, _, _, _)).WillOnce(Return(IndexPartition::OS_FILEIO_EXCEPTION));
    EXPECT_CALL(*indexPart, GetReader()).WillRepeatedly(Return(readerPtr));
    EXPECT_CALL(*reader, GetVersion()).Times(0);
    EXPECT_CALL(*part, loadTableConfig(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*part, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter2))));

    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    checkTable(table3, TS_FORCE_RELOAD, 1, DS_DEPLOYDONE);
    ASSERT_FALSE(tm->getTable(TableMetaUtil::makePidFromStr("t1_2")));

    auto indexPart2 = MockIndexPartition::MakeNice();
    IndexPartitionPtr partPtr2(indexPart2);
    auto reader2 = new NiceMockIndexPartitionReader(_schema);
    IndexPartitionReaderPtr readerPtr2(reader2);
    auto indexPartAdapter3 = std::make_unique<IndexPartitionAdapter>("", "", partPtr2);
    EXPECT_CALL(*indexPart2, Open(_, _, _, _, _)).WillOnce(Return(IndexPartition::OS_FILEIO_EXCEPTION));
    EXPECT_CALL(*indexPart2, GetReader()).WillRepeatedly(Return(readerPtr2));
    EXPECT_CALL(*reader2, GetVersion()).Times(0);
    EXPECT_CALL(*part, loadTableConfig(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*part, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter3))));

    ret = tm->update(target3, target3, false, true);
    ASSERT_EQ(UR_ERROR, ret);
}

TEST_F(TableManagerTest, testRawFileTable) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config_/t1/index_rawfile"}});
    updateTargetReached(tm.get(), target);

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    checkTable(table, TS_LOADED, 1, DS_DEPLOYDONE);
    auto part = dynamic_pointer_cast<NiceMockSuezRawFilePartition>(table->_suezPartition);
    ASSERT_TRUE(part);
    auto data = part->getPartitionData();
    ASSERT_TRUE(part->isInUse());

    auto ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_REACH_TARGET, ret);

    // deploy
    auto target2 = prepareTarget({{"t1", "2_/t1/config_/t1/index_rawfile"}});
    EXPECT_CALL(*part, doDeploy(_, _)).WillOnce(Return(DS_CANCELLED));
    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true));
    checkTable(table, TS_LOADED, 2, DS_CANCELLED);

    EXPECT_CALL(*part, doDeploy(_, _)).WillOnce(Return(DS_FAILED));
    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true));
    checkTable(table, TS_LOADED, 2, DS_FAILED);

    EXPECT_CALL(*part, doDeploy(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);
    ASSERT_EQ("/t1/index", table->_partitionMeta->getIndexRoot());
    ASSERT_EQ("/t1/config", table->_partitionMeta->getConfigPath());

    // load error
    EXPECT_CALL(*part, doLoad(_)).WillOnce(Return(TS_ERROR_UNKNOWN));
    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true));

    // forceload
    EXPECT_CALL(*part, doLoad(_)).WillOnce(Return(TS_LOADED));
    data.reset();
    ASSERT_FALSE(part->isInUse());

    ASSERT_EQ(UR_ERROR, tm->update(target2, target2, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);
    ASSERT_EQ(2, table->_partitionMeta->getIncVersion());

    ASSERT_EQ(UR_REACH_TARGET, tm->update(target2, target2, false, true));
}

TEST_F(TableManagerTest, testDisableForceLoad) {
    auto tm = makeInitedTM();
    tm->_scheduleConfig.allowForceLoad = false;
    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);
    auto data = dynamic_pointer_cast<SuezIndexPartitionData>(part->getPartitionData());
    ASSERT_TRUE(data);
    ASSERT_TRUE(part->isInUse());

    auto target2 = prepareTarget({{"t1", "2_/t1/config1_/t1/index1"}});
    EXPECT_CALL(*part, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));

    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);

    auto pid = TableMetaUtil::makePid("t1");
    ASSERT_TRUE(_indexMap.count(pid) == 1);
    auto indexPartition = _indexMap[pid];
    ASSERT_TRUE(_readerMap.count(pid) == 1);
    auto reader = _readerMap[pid];

    EXPECT_CALL(*indexPartition, ReOpen(false, 2)).WillOnce(Return(IndexPartition::OS_LACK_OF_MEMORY));
    EXPECT_CALL(*reader, GetVersion()).Times(0);
    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true));

    EXPECT_CALL(*indexPartition, ReOpen(false, 2)).WillOnce(Return(IndexPartition::OS_LACK_OF_MEMORY));
    EXPECT_CALL(*reader, GetVersion()).Times(0);
    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true));
}

TEST_F(TableManagerTest, testDisableForceLoadReload) {
    auto tm = makeInitedTM();
    tm->_scheduleConfig.allowForceLoad = false;
    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);

    auto target2 = prepareTarget({{"t1", "0_/t1/config1_/t1/index1"}});

    EXPECT_CALL(*part, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true));
    checkTable(table, TS_LOADED, 0, DS_DEPLOYDONE);

    auto data = dynamic_pointer_cast<SuezIndexPartitionData>(part->getPartitionData());
    ASSERT_TRUE(data);
    ASSERT_TRUE(part->isInUse());
    auto pid = TableMetaUtil::makePid("t1");
    ASSERT_TRUE(_indexMap.count(pid) == 1);
    auto indexPartition = _indexMap[pid];
    ASSERT_TRUE(_readerMap.count(pid) == 1);
    auto reader = _readerMap[pid];

    EXPECT_CALL(*indexPartition, ReOpen(false, 0)).WillOnce(Return(IndexPartition::OS_INDEXLIB_EXCEPTION));
    EXPECT_CALL(*reader, GetVersion()).Times(0);
    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true));

    EXPECT_CALL(*indexPartition, ReOpen(false, 0)).WillOnce(Return(IndexPartition::OS_INDEXLIB_EXCEPTION));
    EXPECT_CALL(*reader, GetVersion()).Times(0);
    ASSERT_EQ(UR_NEED_RETRY, tm->update(target2, target2, false, true));
}

TEST_F(TableManagerTest, testFinalTarget) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);
    auto data = dynamic_pointer_cast<SuezIndexPartitionData>(part->getPartitionData());
    ASSERT_TRUE(data);
    ASSERT_TRUE(part->isInUse());

    auto finalTarget = prepareTarget({{"t1", "2_/t1/config1_/t1/index1"}, {"t2", "1_/t2/config1_/t2/index1"}});

    EXPECT_CALL(*part, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);
    auto table2 = tm->getTable(TableMetaUtil::makePidFromStr("t2"));
    ASSERT_TRUE(table2);
    checkTable(table2, TS_UNLOADED, 1, DS_UNKNOWN);

    auto part2 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table2->_suezPartition);
    ASSERT_TRUE(part2 != nullptr);

    auto pid = TableMetaUtil::makePid("t1");
    ASSERT_TRUE(_indexMap.count(pid) == 1);
    auto indexPartition = _indexMap[pid];
    ASSERT_TRUE(_readerMap.count(pid) == 1);
    auto reader = _readerMap[pid];

    EXPECT_CALL(*indexPartition, ReOpen(_, _)).Times(0);
    EXPECT_CALL(*reader, GetVersion()).Times(0);
    EXPECT_CALL(*part2, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part2, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));
    ASSERT_EQ(1, table->_partitionMeta->getIncVersion());
    checkTable(table2, TS_UNLOADED, 1, DS_DEPLOYDONE);
    ASSERT_EQ(-1, table2->_partitionMeta->getIncVersion());

    EXPECT_CALL(*indexPartition, ReOpen(false, 2)).WillOnce(Return(IndexPartition::OS_OK));
    EXPECT_CALL(*reader, GetVersion()).WillOnce(Return(makeIndexVersion(2)));

    auto indexPartAdapter = makeIndexPartitionAdapterWithVersion(pid, 1);
    EXPECT_CALL(*part2, loadTableConfig(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*part2, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter))));
    ASSERT_EQ(UR_NEED_RETRY, tm->update(finalTarget, finalTarget, false, true));

    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);
    ASSERT_EQ(2, table->_partitionMeta->getIncVersion());
    checkTable(table2, TS_LOADED, 1, DS_DEPLOYDONE);
    ASSERT_EQ(1, table2->_partitionMeta->getIncVersion());

    EXPECT_CALL(*part2, isRecovered()).WillRepeatedly(Return(true));
    ASSERT_EQ(UR_REACH_TARGET, tm->update(finalTarget, finalTarget, false, true));
}

TEST_F(TableManagerTest, testFinalTargetNoDistDp) {
    auto tm = makeInitedTM();
    tm->_scheduleConfig.allowReloadByConfig = true;
    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);
    NiceMockIndexPartition *indexPartition = NiceMockIndexPartition::MakeNice();
    auto data = dynamic_pointer_cast<SuezIndexPartitionData>(part->getPartitionData());
    ASSERT_TRUE(data);
    ASSERT_TRUE(part->isInUse());
    data->_indexPartition.reset(indexPartition);

    auto finalTarget = prepareTarget({{"t1", "1_/t1/config2_/t1/index2"}});
    EXPECT_CALL(*part, doDeployConfig(_, _)).Times(0);
    EXPECT_CALL(*part, doDeployIndex(_, _)).Times(0);
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));

    auto finalTarget2 = prepareTarget({{"t1", "2_/t1/config2_/t1/index2"}});
    EXPECT_CALL(*part, doDeployConfig(_, _)).Times(0);
    EXPECT_CALL(*part, doDeployIndex(_, _)).Times(0);
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget2, false, true));

    EXPECT_CALL(*part, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_EQ(UR_NEED_RETRY, tm->update(finalTarget2, finalTarget2, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);
    ASSERT_EQ("/t1/config2", table->_partitionMeta->getConfigPath());
    ASSERT_EQ("/t1/index2", table->_partitionMeta->getIndexRoot());
}

TEST_F(TableManagerTest, testFinalTargetPreload) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);
    auto data = dynamic_pointer_cast<SuezIndexPartitionData>(part->getPartitionData());
    ASSERT_TRUE(data);
    ASSERT_TRUE(part->isInUse());

    auto finalTarget = prepareTarget({{"t1", "2_/t1/config1_/t1/index1"}}, false, true);

    EXPECT_CALL(*part, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);

    auto pid = TableMetaUtil::makePid("t1");
    ASSERT_TRUE(_indexMap.count(pid) == 1);
    auto indexPartition = _indexMap[pid];
    ASSERT_TRUE(_readerMap.count(pid) == 1);
    auto reader = _readerMap[pid];

    EXPECT_CALL(*indexPartition, ReOpen(false, 2)).WillOnce(Return(IndexPartition::OS_OK));
    EXPECT_CALL(*reader, GetVersion()).WillOnce(Return(makeIndexVersion(2)));

    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);
    ASSERT_EQ(2, table->_partitionMeta->getIncVersion());

    ASSERT_EQ(UR_REACH_TARGET, tm->update(finalTarget, finalTarget, false, true));
}

TEST_F(TableManagerTest, testRollingFreeOperations) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);

    // update inc and keep_count at the same time
    auto finalTarget = prepareTarget({{"t1", "2_/t1/config1_/t1/index1_indexlib_2"}}, false, true);
    ASSERT_EQ(2, finalTarget._tableMetas[table->getPid()].getKeepCount());
    EXPECT_CALL(*part, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));

    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));
    ASSERT_EQ(2, table->_partitionMeta->getKeepCount());
}

TEST_F(TableManagerTest, testTableReaderEmpty) {
    TableManager tm;
    tm.init(_initParam);

    HeartbeatTarget target;
    MultiTableReader tableReader;
    ASSERT_TRUE(tm.getTableReader(target, tableReader));
    EXPECT_EQ(0, tableReader.getIndexlibTableReaders().size());
}

TEST_F(TableManagerTest, testTableReaderSingle) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config_/t1/index"}, {"t2", "1_/t2/config_/t2/index_rawfile"}});
    updateTargetReached(tm.get(), target);

    MultiTableReader tableReader;
    ASSERT_TRUE(tm->getTableReader(target, tableReader));
    EXPECT_EQ(1, tableReader.getIndexlibTableReaders().size());
}

TEST_F(TableManagerTest, testTableReaderCanServe) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config_/t1/index_indexlib"}});

    updateTargetReached(tm.get(), target);

    MultiTableReader tableReader;
    ASSERT_TRUE(tm->getTableReader(target, tableReader));
    EXPECT_EQ(1, tableReader.getIndexlibTableReaders().size());

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    table->_partitionMeta->setConfigPath("/t1/config2");
    ASSERT_FALSE(tm->getTableReader(target, tableReader));
    EXPECT_EQ(0, tableReader.getIndexlibTableReaders().size());
    table->_partitionMeta->setLoadedConfigPath("/t1/config2");

    table->_partitionMeta->setIndexRoot("/t1/index2");
    ASSERT_FALSE(tm->getTableReader(target, tableReader));
    EXPECT_EQ(0, tableReader.getIndexlibTableReaders().size());
    table->_partitionMeta->setLoadedIndexRoot("/t1/index2");

    ASSERT_TRUE(tm->getTableReader(target, tableReader));
    EXPECT_EQ(1, tableReader.getIndexlibTableReaders().size());

    auto availableTs = {TS_LOADING, TS_PRELOADING, TS_PRELOAD_FAILED, TS_PRELOAD_FORCE_RELOAD};
    auto errorTs = {TS_ERROR_UNKNOWN, TS_FORCELOADING};
    for (const auto &ts : availableTs) {
        table->setTableStatus(ts);
        ASSERT_TRUE(tm->getTableReader(target, tableReader));
        EXPECT_EQ(1, tableReader.getIndexlibTableReaders().size());
    }
    for (const auto &ts : errorTs) {
        table->setTableStatus(ts);
        ASSERT_FALSE(tm->getTableReader(target, tableReader));
        EXPECT_EQ(0, tableReader.getIndexlibTableReaders().size());
    }
    table->_partitionMeta->setFullIndexLoaded(false);
    for (const auto &ts : availableTs) {
        table->setTableStatus(ts);
        ASSERT_FALSE(tm->getTableReader(target, tableReader));
        EXPECT_EQ(0, tableReader.getIndexlibTableReaders().size());
    }
}

TEST_F(TableManagerTest, testTableReaderRawFile) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config_/t1/index_rawfile"}});

    updateTargetReached(tm.get(), target);

    MultiTableReader tableReader;
    ASSERT_TRUE(tm->getTableReader(target, tableReader));
    EXPECT_EQ(0, tableReader.getIndexlibTableReaders().size());

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    table->setTableStatus(TS_LOADING);
    ASSERT_FALSE(tm->getTableReader(target, tableReader));
    EXPECT_EQ(0, tableReader.getIndexlibTableReaders().size());
}

TEST_F(TableManagerTest, testGetTableReaderMulti) {
    auto tm = makeInitedTM();
    auto target = prepareTarget(
        {{"t1", "1_/t1/config_/t1/index"}, {"t2", "2_/t2/config_/t2/index"}, {"t3", "3_/t3/config_/t3/index"}});
    updateTargetReached(tm.get(), target);

    MultiTableReader tableReader;
    ASSERT_TRUE(tm->getTableReader(target, tableReader));
    EXPECT_EQ(3, tableReader.getIndexlibTableReaders().size());

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    table->setTableStatus(TS_ERROR_UNKNOWN);

    ASSERT_FALSE(tm->getTableReader(target, tableReader));
    EXPECT_EQ(2, tableReader.getIndexlibTableReaders().size());
}

TEST_F(TableManagerTest, testFinalToTargetPreloadFail) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);
    auto data = dynamic_pointer_cast<SuezIndexPartitionData>(part->getPartitionData());
    ASSERT_TRUE(data);
    ASSERT_TRUE(part->isInUse());

    auto finalTarget = prepareTarget({{"t1", "2_/t1/config1_/t1/index1"}}, false, true);

    EXPECT_CALL(*part, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);

    auto pid = TableMetaUtil::makePid("t1");
    ASSERT_TRUE(_indexMap.count(pid) == 1);
    auto indexPartition = _indexMap[pid];
    ASSERT_TRUE(_readerMap.count(pid) == 1);
    auto reader = _readerMap[pid];

    EXPECT_CALL(*indexPartition, ReOpen(false, 2)).WillOnce(Return(IndexPartition::OS_LACK_OF_MEMORY));
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));
    checkTable(table, TS_PRELOAD_FAILED, 2, DS_DEPLOYDONE);
    ASSERT_EQ(1, table->_partitionMeta->getIncVersion());
    MultiTableReader tableReader;
    ASSERT_TRUE(tm->getTableReader(target, tableReader));
    ASSERT_EQ(1, tableReader.getIndexlibTableReaders().size());

    // force load fail because use count > 1, wait stop service
    ASSERT_EQ(UR_NEED_RETRY, tm->update(finalTarget, finalTarget, false, true));
    checkTable(table, TS_ERROR_UNKNOWN, 2, DS_DEPLOYDONE);

    ASSERT_FALSE(tm->getTableReader(finalTarget, tableReader));
    EXPECT_EQ(0, tableReader.getIndexlibTableReaders().size());

    // force load
    EXPECT_CALL(*indexPartition, ReOpen(true, 2)).WillOnce(Return(IndexPartition::OS_OK));
    EXPECT_CALL(*reader, GetVersion()).WillOnce(Return(makeIndexVersion(2)));

    data.reset();
    ASSERT_EQ(UR_ERROR, tm->update(finalTarget, finalTarget, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);

    ASSERT_TRUE(tm->getTableReader(finalTarget, tableReader));
    EXPECT_EQ(1, tableReader.getIndexlibTableReaders().size());
    ASSERT_EQ(UR_REACH_TARGET, tm->update(finalTarget, finalTarget, false, true));
}

TEST_F(TableManagerTest, testFinalToTargetPreloadForceReload) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);
    auto data = dynamic_pointer_cast<SuezIndexPartitionData>(part->getPartitionData());
    ASSERT_TRUE(data);
    ASSERT_TRUE(part->isInUse());

    auto finalTarget = prepareTarget({{"t1", "2_/t1/config1_/t1/index1"}}, false, true);

    EXPECT_CALL(*part, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);

    auto pid = TableMetaUtil::makePid("t1");
    ASSERT_TRUE(_indexMap.count(pid) == 1);
    auto indexPartition = _indexMap[pid];

    EXPECT_CALL(*indexPartition, ReOpen(false, 2)).WillOnce(Return(IndexPartition::OS_INDEXLIB_EXCEPTION));
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));
    checkTable(table, TS_PRELOAD_FORCE_RELOAD, 2, DS_DEPLOYDONE);
    ASSERT_EQ(1, table->_partitionMeta->getIncVersion());
    MultiTableReader tableReader;
    ASSERT_TRUE(tm->getTableReader(target, tableReader));
    ASSERT_EQ(1, tableReader.getIndexlibTableReaders().size());

    // force load fail because use count > 1, wait stop service
    ASSERT_EQ(UR_NEED_RETRY, tm->update(finalTarget, finalTarget, false, true));
    checkTable(table, TS_FORCE_RELOAD, 2, DS_DEPLOYDONE);

    ASSERT_FALSE(tm->getTableReader(finalTarget, tableReader));
    EXPECT_EQ(0, tableReader.getIndexlibTableReaders().size());

    // force re load
    auto indexPartAdapter = makeIndexPartitionAdapterWithVersion(pid, 2);
    EXPECT_CALL(*part, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter))));
    data.reset();

    ASSERT_EQ(UR_ERROR, tm->update(finalTarget, finalTarget, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);

    ASSERT_TRUE(tm->getTableReader(finalTarget, tableReader));
    EXPECT_EQ(1, tableReader.getIndexlibTableReaders().size());
    ASSERT_EQ(UR_REACH_TARGET, tm->update(finalTarget, finalTarget, false, true));
}

TEST_F(TableManagerTest, testFinalToTargetPreloading) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);

    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);
    NiceMockIndexPartition *indexPartition = NiceMockIndexPartition::MakeNice();
    auto data = dynamic_pointer_cast<SuezIndexPartitionData>(part->getPartitionData());
    ASSERT_TRUE(data);
    ASSERT_TRUE(part->isInUse());
    data->_indexPartition.reset(indexPartition);

    auto finalTarget = prepareTarget({{"t1", "2_/t1/config1_/t1/index1"}}, false, true);

    EXPECT_CALL(*part, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_EQ(UR_REACH_TARGET, tm->update(target, finalTarget, false, true));
    checkTable(table, TS_LOADED, 2, DS_DEPLOYDONE);
    table->setTableStatus(TS_PRELOADING);

    ASSERT_EQ(UR_NEED_RETRY, tm->update(finalTarget, finalTarget, false, true));
    checkTable(table, TS_LOADING, 2, DS_DEPLOYDONE);

    MultiTableReader tableReader;
    ASSERT_TRUE(tm->getTableReader(finalTarget, tableReader));
    ASSERT_EQ(1, tableReader.getIndexlibTableReaders().size());
}

TEST_F(TableManagerTest, testEnforceUnloadLoadOrder) {
    auto tm = makeInitedTM();
    auto v1 = TableMetaUtil::makeTableMeta("t1_1", "1_/t1/config1_/t1/index1");
    auto v2 = TableMetaUtil::makeTableMeta("t1_2", "1_/t1/config2_/t1/index2");
    auto &pid1 = v1.first;
    auto &meta1 = v1.second;
    meta1.setTableLoadType(TLT_UNLOAD_FIRST);
    auto &pid2 = v2.first;
    auto &meta2 = v2.second;
    meta2.setTableLoadType(TLT_UNLOAD_FIRST);

    TableMetas tableMetas;
    tableMetas[pid1] = meta1;
    tableMetas[pid2] = meta2;

    auto t1 = tm->createTable(pid1, meta1);
    ASSERT_TRUE(t1);
    auto t2 = tm->createTable(pid2, meta2);
    ASSERT_TRUE(t2);

    TodoList todoList;
    todoList.addOperation(t2, OP_LOAD, meta2);
    ASSERT_FALSE(tm->enforceUnloadLoadOrder(tableMetas, todoList));
    ASSERT_EQ(1, todoList.size());

    t1->setTableStatus(TS_LOADED);
    todoList.addOperation(t1, OP_UNLOAD);
    ASSERT_EQ(2, todoList.size());
    ASSERT_TRUE(tm->enforceUnloadLoadOrder(tableMetas, todoList));
    ASSERT_EQ(1, todoList.size());
    ASSERT_TRUE(todoList.getOperations(pid2).empty());
    ASSERT_EQ(1, todoList.getOperations(pid1).size());
}

TEST_F(TableManagerTest, testFinalTargetAndTargetHaveSameTableDiffGenerationDoUnloadCauseLackMem) {
    auto tm = makeInitedTM();
    auto target = prepareTarget({{"t1_0", "1_/t1/config_/t1/index"}, {"t1_1", "1_/t1/config_/t1/index"}});
    updateTargetReached(tm.get(), target);
    auto target1 = prepareTarget({{"t1_0", "1_/t1/config_/t1/index"}});
    auto final1 = prepareTarget({{"t1_1", "1_/t1/config_/t1/index"}});
    while (UR_REACH_TARGET != tm->update(target1, final1, false))
        ;
    auto tableMetas = tm->getTableMetas();
    EXPECT_EQ(2u, tableMetas.size());
    EXPECT_EQ(TS_LOADED, tableMetas[TableMetaUtil::makePidFromStr("t1_0")].getTableStatus());
    EXPECT_EQ(TS_UNLOADED, tableMetas[TableMetaUtil::makePidFromStr("t1_1")].getTableStatus());
}

TEST_F(TableManagerTest, testNeedCleanIncVersion) {
    TableManager tableManager;
    tableManager._scheduleConfig.allowFastCleanIncVersion = false;
    ASSERT_TRUE(!tableManager.needCleanIncVersion());
    tableManager._scheduleConfig.allowFastCleanIncVersion = true;
    tableManager._scheduleConfig.fastCleanIncVersionIntervalInSec = 1;
    ASSERT_TRUE(tableManager.needCleanIncVersion());
    ASSERT_TRUE(!tableManager.needCleanIncVersion());
    sleep(2);
    ASSERT_TRUE(tableManager.needCleanIncVersion());
    ASSERT_TRUE(!tableManager.needCleanIncVersion());
}

TEST_F(TableManagerTest, testGenTargetLayout) {
    auto tm = makeInitedTM();
    // table, generation_0, partition_0_32767, inc_version 0
    auto currentMetas = TableMetaUtil::makeTableMetas({"simple_0_0_32767"}, {"0_/config_/index"});
    auto target = prepareTarget({{"simple_1_32768_65535", "0_/config_/index"}});
    auto targetMetas = target.getTableMetas();

    // layout record 2 generations both
    TargetLayout layout;
    tm->genTargetLayout(layout, currentMetas);
    tm->genTargetLayout(layout, targetMetas);

    ASSERT_EQ(2, layout._baseMap.size());
    auto indexDirectory = layout._baseMap.begin()->second;
    ASSERT_TRUE(indexDirectory != nullptr);
    ASSERT_TRUE(indexDirectory->_subDirs.find("simple") != indexDirectory->_subDirs.end());
    auto tableDirectory = indexDirectory->_subDirs["simple"];
    ASSERT_TRUE(tableDirectory != nullptr);
    auto genDirectories = tableDirectory->_subDirs;
    vector<string> generations = {"generation_0", "generation_1"};
    ASSERT_TRUE(genDirectories.find(generations[0]) != genDirectories.end());
    ASSERT_TRUE(genDirectories.find(generations[1]) != genDirectories.end());
}

TEST_F(TableManagerTest, testTriggerCleanIncVersionOperation) {
    auto tm = makeInitedTM();
    tm->_scheduleConfig.allowFastCleanIncVersion = true;
    tm->_scheduleConfig.fastCleanIncVersionIntervalInSec = 100;
    ASSERT_TRUE(tm->needCleanIncVersion());

    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    auto targetMetas = target.getTableMetas();

    auto v = TableMetaUtil::makeTableMeta("t1", "1_/t1/config2_/t1/index2");
    auto &pid = v.first;
    auto &meta = v.second;
    meta.setTableLoadType(TLT_UNLOAD_FIRST);

    TableMetas tableMetas;
    tableMetas[pid] = meta;

    auto t = tm->createTable(pid, meta);
    ASSERT_TRUE(t);

    // not reach interval
    {
        TodoList todoList;
        todoList.addOperation(t, OP_LOAD, meta);
        ASSERT_FALSE(tm->needCleanIncVersion());
        tm->generateCleanIncVersionOperations(target, target, todoList);
        ASSERT_EQ(1, todoList.size());
    }

    // trigger by OP_CLEAN_DISK
    {
        TodoList todoList;
        todoList.addOperation(t, OP_CLEAN_DISK, meta);
        tm->generateCleanIncVersionOperations(target, target, todoList);

        // OP_CLEAN_INC_VERSION in todo
        ASSERT_EQ(2, todoList.size());
        auto todoVec = todoList.getOperations(pid);
        auto toCleanIncVersion = dynamic_cast<CleanIncVersion *>(todoVec[1]);
        ASSERT_TRUE(toCleanIncVersion != nullptr);
    }

    // trigger by loop interval
    tm->_nextCleanIncVersionTime = TimeUtility::currentTime();
    tm->_scheduleConfig.fastCleanIncVersionIntervalInSec = 1;
    sleep(2);
    {
        TodoList todoList;
        todoList.addOperation(t, OP_LOAD, meta);
        tm->generateCleanIncVersionOperations(target, target, todoList);
        ASSERT_EQ(2, todoList.size());
    }
}

TEST_F(TableManagerTest, testGenerateCommitOperations) {
    auto manager = makeInitedTM();
    TodoList todoList;
    auto pid = TableMetaUtil::makePidFromStr("t1");
    auto target = TableMetaUtil::makeTargetFromStr("1_/t1/config_/t1/index_indexlib_1_rw");
    TableMetas tableMetas;
    tableMetas[pid] = target;

    // table does not exist ?
    manager->generateCommitOperations(tableMetas, todoList);
    ASSERT_EQ(0, todoList.getOperations().size());

    manager->createTable(pid, target);

    // table status != TS_LOADED
    auto table = manager->getTable(pid);
    ASSERT_TRUE(table.get() != nullptr);
    manager->generateCommitOperations(tableMetas, todoList);
    ASSERT_EQ(0, todoList.getOperations().size());

    // dont need commit
    table->setTableStatus(TS_LOADED);
    auto mockPart =
        std::make_shared<NiceMockSuezPartition>(table->_partitionMeta, pid, table->_tableResource.metricsReporter);
    table->_suezPartition = mockPart;
    EXPECT_CALL(*(mockPart.get()), needCommit()).WillOnce(Return(false));
    manager->generateCommitOperations(tableMetas, todoList);
    ASSERT_EQ(0, todoList.getOperations().size());

    // add one commit operation
    EXPECT_CALL(*(mockPart.get()), needCommit()).WillOnce(Return(true));
    manager->generateCommitOperations(tableMetas, todoList);
    ASSERT_EQ(1, todoList.getOperations().size());
    ASSERT_TRUE(todoList.hasOpType(OP_COMMIT));
}

TEST_F(TableManagerTest, testGenerateCleanIncVersion) {
    auto tm = makeInitedTM();
    tm->_scheduleConfig.allowFastCleanIncVersion = true;
    tm->_nextCleanIncVersionTime = TimeUtility::currentTime();
    tm->_scheduleConfig.fastCleanIncVersionIntervalInSec = 0; // always need cleanIncVersion

    auto pid = TableMetaUtil::makePidFromStr("t1");
    auto table = tm->createTable(pid, PartitionMeta());
    ASSERT_TRUE(table);
    auto current = table->_partitionMeta;

    {
        // cur:-1, tar:1, dpMap:null
        ASSERT_EQ(INVALID_VERSION, current->getIncVersion());
        auto target = prepareTarget({{"t1", "1"}});
        ASSERT_EQ(0, current->deployMeta->deployStatusMap.size());

        TodoList todoList;
        tm->generateCleanIncVersionOperations(target, target, todoList);
        auto todoVec = todoList.getOperations(pid);
        ASSERT_EQ(1, todoVec.size());
        auto toCleanIncVersion = dynamic_cast<CleanIncVersion *>(todoVec[0]);
        ASSERT_TRUE(toCleanIncVersion != nullptr);
        checkInUseVersions({-1, 1}, toCleanIncVersion);
    }

    {
        // cur:-1, tar:2, dpMap:1
        ASSERT_EQ(INVALID_VERSION, current->getIncVersion());
        auto target = prepareTarget({{"t1", "2"}});
        current->setDeployStatus(1, DS_DEPLOYDONE);
        ASSERT_EQ(1, current->deployMeta->deployStatusMap.size());

        TodoList todoList;
        tm->generateCleanIncVersionOperations(target, target, todoList);
        auto todoVec = todoList.getOperations(pid);
        ASSERT_EQ(1, todoVec.size());
        auto toCleanIncVersion = dynamic_cast<CleanIncVersion *>(todoVec[0]);
        ASSERT_TRUE(toCleanIncVersion != nullptr);

        checkInUseVersions({-1, 1, 2}, toCleanIncVersion);
    }

    {
        // rollback
        // cur:1, tar: 1, dpMap:1, 2
        current->setIncVersion(1);
        auto target = prepareTarget({{"t1", "1"}});
        current->setDeployStatus(1, DS_DEPLOYDONE);
        current->setDeployStatus(2, DS_DEPLOYDONE);
        ASSERT_EQ(2, current->deployMeta->deployStatusMap.size());

        TodoList todoList;
        tm->generateCleanIncVersionOperations(target, target, todoList);
        auto todoVec = todoList.getOperations(pid);
        ASSERT_EQ(1, todoVec.size());
        auto toCleanIncVersion = dynamic_cast<CleanIncVersion *>(todoVec[0]);
        ASSERT_TRUE(toCleanIncVersion != nullptr);

        checkInUseVersions({1, 2}, toCleanIncVersion);
    }

    {
        // cur:2, tar:4, finalTar: 5, dpMap:1,2,3,
        current->setIncVersion(2);
        auto target = prepareTarget({{"t1", "4"}});
        auto finalTarget = prepareTarget({{"t1", "5"}});
        current->setDeployStatus(1, DS_DEPLOYDONE);
        current->setDeployStatus(2, DS_DEPLOYDONE);
        current->setDeployStatus(3, DS_DEPLOYING);
        ASSERT_EQ(3, current->deployMeta->deployStatusMap.size());

        TodoList todoList;
        tm->generateCleanIncVersionOperations(target, finalTarget, todoList);
        auto todoVec = todoList.getOperations(pid);
        ASSERT_EQ(1, todoVec.size());
        auto toCleanIncVersion = dynamic_cast<CleanIncVersion *>(todoVec[0]);
        ASSERT_TRUE(toCleanIncVersion != nullptr);

        checkInUseVersions({2, 3, 4, 5}, toCleanIncVersion);
    }
}

TEST_F(TableManagerTest, testCleanIncVersion) {
    auto tm = makeInitedTM();
    tm->_scheduleConfig.allowFastCleanIncVersion = true;
    tm->_nextCleanIncVersionTime = TimeUtility::currentTime();
    tm->_scheduleConfig.fastCleanIncVersionIntervalInSec = 0; // always need cleanIncVersion

    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    updateTargetReached(tm.get(), target);
    auto table = tm->getTable(TableMetaUtil::makePidFromStr("t1"));
    ASSERT_TRUE(table);
    auto current = table->_partitionMeta;
    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);

    EXPECT_CALL(*part, cleanIndexFiles(_)).Times(1);
    auto ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_REACH_TARGET, ret);
}

TEST_F(TableManagerTest, testCleanIncVersionWithDeploy) {
    auto tm = makeInitedTM();
    tm->_scheduleConfig.allowFastCleanIncVersion = true;
    tm->_nextCleanIncVersionTime = TimeUtility::currentTime();
    tm->_scheduleConfig.fastCleanIncVersionIntervalInSec = 0; // always need cleanIncVersion

    auto target = prepareTarget({{"t1", "1_/t1/config1_/t1/index1"}});
    auto pid = TableMetaUtil::makePidFromStr("t1");

    // init
    auto ret = tm->update(target, target, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    auto table = tm->getTable(pid);
    ASSERT_TRUE(table);

    auto part = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table->_suezPartition);
    ASSERT_TRUE(part != nullptr);

    // dp
    EXPECT_CALL(*(part), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part), doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*part, cleanIndexFiles(_)).Times(0);
    ASSERT_EQ(DS_UNKNOWN, table->getTableDeployStatus(1));
    ret = tm->update(target, target, false, false);
    ASSERT_EQ(UR_NEED_RETRY, ret);

    // wait deploy done
    int retryCount = 10;
    while (retryCount > 0) {
        auto dpStatus = table->getTableDeployStatus(1);
        if (dpStatus != DS_DEPLOYDONE) {
            --retryCount;
            continue;
        } else {
            break;
        }
    }
}

TEST_F(TableManagerTest, testCleanFullVersion) {
    auto tm = makeInitedTM();
    auto tmpPath = GET_TEST_DATA_PATH() + "/table_test/testCleanFullVersion_temp_dir/";
    auto indexRoot = tmpPath + "runtimedata/";
    auto configPath = tmpPath + "zone_config/";
    tm->_localIndexRoot = indexRoot;
    tm->_localTableConfigBaseDir = configPath;
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(indexRoot + "simple/generation_0", true));
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(indexRoot + "tableNeedClean", true));

    // current is : simple, generation 0, partition 0_65535, inc_version 0
    auto target0 = prepareTarget({{"simple_0_0_65535", "0_/simple/config/_/simple/index"}});
    updateTargetReached(tm.get(), target0);
    auto pid0 = TableMetaUtil::makePid("simple", 0, 0, 65535);
    auto table0 = tm->getTable(pid0);
    ASSERT_TRUE(table0);
    auto part0 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table0->_suezPartition);
    ASSERT_TRUE(part0 != nullptr);
    // generation 0 hold by search service
    SuezPartitionDataPtr holdBySearchService = part0->getPartitionData();
    ASSERT_EQ(1, part0->getUseCount());

    // target : simple, generation 1, partition 0_65535, inc_version 0, partition type indexlib, keepCount=0
    auto target1 = prepareTarget({{"simple_1_0_65535", "0_/simple/config/_/simple/index_indexlib_0"}});
    auto ret = tm->update(target1, target1, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    auto pid1 = TableMetaUtil::makePid("simple", 1, 0, 65535);
    auto table1 = tm->getTable(pid1);
    ASSERT_TRUE(table1);
    auto part1 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table1->_suezPartition);
    ASSERT_TRUE(part1 != nullptr);
    checkTable(table1, TS_UNLOADED, 1);
    ASSERT_EQ(2, tm->getTableMetas().size());

    // when dp generation 1, reach disk quota
    EXPECT_CALL(*(part1), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part1), doDeployIndex(_, _)).WillOnce(Return(DS_DISKQUOTA));
    ret = tm->update(target1, target1, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    ASSERT_EQ(fslib::EC_TRUE, fslib::fs::FileSystem::isExist(indexRoot + "tableNeedClean"));

    // cleaned unused table success, will retry
    EXPECT_CALL(*(part1), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part1), doDeployIndex(_, _)).WillOnce(Return(DS_DISKQUOTA));
    ret = tm->update(target1, target1, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    ASSERT_EQ(fslib::EC_FALSE, fslib::fs::FileSystem::isExist(indexRoot + "tableNeedClean"));

    // nothing could clean, will stop service
    EXPECT_CALL(*(part1), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part1), doDeployIndex(_, _)).WillOnce(Return(DS_DISKQUOTA));
    ret = tm->update(target1, target1, false, true);
    ASSERT_EQ(UR_ERROR, ret);

    // search service release, unload generation 0
    holdBySearchService = nullptr;
    ASSERT_EQ(0, part0->getUseCount());
    EXPECT_CALL(*(part1), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part1), doDeployIndex(_, _)).WillOnce(Return(DS_DISKQUOTA));
    ret = tm->update(target1, target1, false, true);

    // clean generation 0, dp success
    EXPECT_CALL(*(part1), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*(part1), doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ret = tm->update(target1, target1, false, true);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    ASSERT_EQ(1, tm->getTableMetas().size());
    ASSERT_EQ(fslib::EC_FALSE, fslib::fs::FileSystem::isExist(indexRoot + "simple/generation_0"));
}

TEST_F(TableManagerTest, testCleanDiskWhenConfigChange) {
    UPDATE_RESULT ret;
    auto tm = makeInitedTM(true);
    auto tmpPath = GET_TEST_DATA_PATH() + "/table_test/testCleanConfig_temp_dir/";
    auto indexRoot = tmpPath + "runtimedata/";
    auto configPath = tmpPath + "zone_config/table/";
    tm->_localIndexRoot = indexRoot;
    tm->_localTableConfigBaseDir = configPath;
    auto dpRunner = dynamic_cast<FakeAsyncTodoRunner *>(getTodoRunner(tm, "deploy"));
    ASSERT_TRUE(dpRunner != nullptr);

    // this target is to load partition : simple, generation 0; daogou, generation 0
    auto target0 =
        prepareTarget({{"simple_0_0_65535", "0_/remote/simple/config/000_/remote/simple/index_indexlib_0"}}, true);
    updateTargetReached(tm.get(), target0);
    auto pid0 = TableMetaUtil::makePid("simple", 0, 0, 65535);
    auto table0 = tm->getTable(pid0);
    ASSERT_TRUE(table0);
    auto part0 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table0->_suezPartition);
    ASSERT_TRUE(part0 != nullptr);

    // simple index deploying
    auto target1 =
        prepareTarget({{"simple_0_0_65535", "1_/remote/simple/config/111_/remote/simple/index_indexlib_0"}}, true);
    ret = tm->update(target1, target1, false, false);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    EXPECT_CALL(*(part0), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(configPath + "simple/111", true));

    auto target2 =
        prepareTarget({{"simple_0_0_65535", "1_/remote/simple/config/222_/remote/simple/index_indexlib_0"}}, true);
    // new target with simple's config change, will clean simple/config1
    ret = tm->update(target2, target2, false, false);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    ASSERT_EQ(fslib::EC_FALSE, fslib::fs::FileSystem::isExist(configPath + "simple/111"));

    // simple dp done, current is simple/config1
    EXPECT_CALL(*(part0), doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_TRUE(dpRunner->finishAsyncTask(pid0));
    ASSERT_EQ("/remote/simple/config/111", tm->getTableMetas()[pid0].getConfigPath());

    dpRunner->stopAll();
}

TEST_F(TableManagerTest, testKeepConfigCount) {
    UPDATE_RESULT ret;
    auto tm = makeInitedTM(true);
    auto tmpPath = GET_TEST_DATA_PATH() + "/table_test/testCleanConfig_temp_dir/";
    auto indexRoot = tmpPath + "runtimedata/";
    auto configPath = tmpPath + "zone_config/table/";
    tm->_localIndexRoot = indexRoot;
    tm->_localTableConfigBaseDir = configPath;
    auto dpRunner = dynamic_cast<FakeAsyncTodoRunner *>(getTodoRunner(tm, "deploy"));
    ASSERT_TRUE(dpRunner != nullptr);

    // this target is to load partition : simple, generation 0; daogou, generation 0
    auto target0 = prepareTarget(
        {{"simple_0_0_65535", "0_/remote/simple/config/000_/remote/simple/index_indexlib_0_normal_1"}}, true);
    updateTargetReached(tm.get(), target0);
    auto pid0 = TableMetaUtil::makePid("simple", 0, 0, 65535);
    auto table0 = tm->getTable(pid0);
    ASSERT_TRUE(table0);
    auto part0 = dynamic_pointer_cast<NiceMockSuezIndexPartition>(table0->_suezPartition);
    ASSERT_TRUE(part0 != nullptr);

    // simple index deploying
    auto target1 = prepareTarget(
        {{"simple_0_0_65535", "1_/remote/simple/config/111_/remote/simple/index_indexlib_0_normal_1"}}, true);
    ret = tm->update(target1, target1, false, false);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    EXPECT_CALL(*(part0), doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    // config download , index deploying
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(configPath + "simple/111", true));

    auto target2 = prepareTarget(
        {{"simple_0_0_65535", "1_/remote/simple/config/222_/remote/simple/index_indexlib_0_normal_1"}}, true);
    // new target with simple's config change, config keep count=1, dont clean config 111

    ret = tm->update(target2, target2, false, false);
    ASSERT_EQ(UR_NEED_RETRY, ret);
    ASSERT_EQ(fslib::EC_TRUE, fslib::fs::FileSystem::isExist(configPath + "simple/111"));

    // simple dp done, current is simple/config1
    EXPECT_CALL(*(part0), doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));
    ASSERT_TRUE(dpRunner->finishAsyncTask(pid0));
    ASSERT_EQ("/remote/simple/config/111", tm->getTableMetas()[pid0].getConfigPath());

    dpRunner->stopAll();
}

} // namespace suez
