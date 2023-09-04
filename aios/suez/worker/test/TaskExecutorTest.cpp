#include "suez/worker/TaskExecutor.h"

#include "indexlib/partition/online_partition.h"
#include "suez/heartbeat/HeartbeatManager.h"
#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/table/IndexPartitionAdapter.h"
#include "suez/table/SuezIndexPartition.h"
#include "suez/table/TableManager.h"
#include "suez/table/test/MockSuezPartition.h"
#include "suez/worker/WorkerCurrent.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace suez {

class TaskExecutorTest : public TESTBASE {
public:
    void setUp() override;
    void prepare(TableMetas &current, TableMetas &target, PartitionId &pid_);

    std::unique_ptr<TaskExecutor> _taskExecutor;
    HeartbeatManager _hbManager;
};

void TaskExecutorTest::setUp() {
    KMonitorMetaInfo metaInfo;
    _taskExecutor = std::make_unique<TaskExecutor>(nullptr, metaInfo);
    _taskExecutor->_tableManager = std::make_unique<TableManager>();
    _taskExecutor->_tableManager->_globalIndexResource = indexlib::partition::PartitionGroupResource::Create(
        1024 * 1024 * 1024, kmonitor::MetricsReporterPtr(), NULL, NULL);
    _taskExecutor->_hbManager = &_hbManager;
}

TEST_F(TaskExecutorTest, testConstructor) {
    ASSERT_EQ(INVALID_TARGET_VERSION, _taskExecutor->_targetVersion);
    ASSERT_FALSE(_taskExecutor->_allowPartialTableReady);
    ASSERT_FALSE(_taskExecutor->_needShutdownGracefully);
    ASSERT_FALSE(_taskExecutor->_shutdownFlag);
    ASSERT_FALSE(_taskExecutor->_shutdownSucc);
}

TEST_F(TaskExecutorTest, testGetIndexProvider) {
    HeartbeatTarget target;

    // tablemanager getTableReader failed.
    PartitionMeta meta;
    meta.incVersion = 0;
    meta.setTableType(SPT_INDEXLIB);
    PartitionId pidA;
    pidA.tableId.tableName = "hpaio_content_feature_table_pb";
    pidA.tableId.fullVersion = 1459203587;

    target._tableMetas[pidA] = meta;
    _taskExecutor->_tableManager->createTable(pidA, meta);
    TablePtr tableA = _taskExecutor->_tableManager->getTable(pidA);
    SuezIndexPartitionPtr suezIndexPartitionA(new SuezIndexPartition(TableResource(pidA), tableA->_partitionMeta));
    auto partition = std::make_shared<indexlib::partition::OnlinePartition>();
    suezIndexPartitionA->_indexlibAdapter = std::make_unique<IndexPartitionAdapter>("", "", partition);
    tableA->_suezPartition = suezIndexPartitionA;
    tableA->_partitionMeta->tableStatus = TS_LOADING;
    tableA->_partitionMeta->tableMeta = meta.tableMeta;
    tableA->_partitionMeta->incVersion = 0;
    // table not ready, not allowPartialTableReady
    _taskExecutor->_allowPartialTableReady = false;
    IndexProviderPtr retIndexProvider = _taskExecutor->getIndexProvider(target);
    ASSERT_TRUE(retIndexProvider);
    ASSERT_FALSE(retIndexProvider->allTableReady);

    // table not ready, allowPartialTableReady
    _taskExecutor->_allowPartialTableReady = true;
    retIndexProvider = _taskExecutor->getIndexProvider(target);
    ASSERT_TRUE(retIndexProvider);
    ASSERT_FALSE(retIndexProvider->allTableReady);

    // table ready (loaded), not allowPartialTableReady
    suezIndexPartitionA->_partitionMeta->setFullIndexLoaded(true);
    tableA->_partitionMeta->tableStatus = TS_LOADED;
    _taskExecutor->_allowPartialTableReady = false;
    retIndexProvider = _taskExecutor->getIndexProvider(target);
    ASSERT_TRUE(retIndexProvider);
    ASSERT_TRUE(retIndexProvider->allTableReady);

    // table can serve (loading), not allowPartialTableReady
    tableA->_partitionMeta->tableStatus = TS_LOADING;
    retIndexProvider = _taskExecutor->getIndexProvider(target);
    ASSERT_TRUE(retIndexProvider);
    ASSERT_TRUE(retIndexProvider->allTableReady);

    // table ready, allowPartialTableReady
    tableA->_partitionMeta->tableStatus = TS_LOADED;
    _taskExecutor->_allowPartialTableReady = true;
    retIndexProvider = _taskExecutor->getIndexProvider(target);
    ASSERT_TRUE(retIndexProvider);
    ASSERT_TRUE(retIndexProvider->allTableReady);
}

TEST_F(TaskExecutorTest, testAdjustWorkerCurrent) {
    PartitionMeta meta;
    meta.incVersion = 0;
    meta.setTableType(SPT_INDEXLIB);
    PartitionId pid;
    pid.tableId.tableName = "tableName";
    pid.tableId.fullVersion = 1459203587;

    _taskExecutor->_tableManager->createTable(pid, meta);
    auto table = _taskExecutor->_tableManager->getTable(pid);
    auto part = std::make_shared<NiceMockSuezPartition>(std::make_shared<CurrentPartitionMeta>(), pid, nullptr);
    table->_suezPartition = part;
    table->_partitionMeta->tableStatus = TS_LOADED;
    table->_partitionMeta->tableMeta = meta.tableMeta;
    table->_partitionMeta->incVersion = 0;

    WorkerCurrent current;
    current.setTableInfos(_taskExecutor->_tableManager->getTableMetas());
    auto metas = current.getTableInfos();
    ASSERT_EQ(size_t(1), metas.size());
    ASSERT_TRUE(TS_LOADED == metas[pid].getTableStatus());
    EXPECT_CALL(*(part), isRecovered()).WillRepeatedly(Return(false));
    _taskExecutor->adjustWorkerCurrent(current);
    metas = current.getTableInfos();
    ASSERT_TRUE(TS_LOADING == metas[pid].getTableStatus());
}

} // namespace suez
