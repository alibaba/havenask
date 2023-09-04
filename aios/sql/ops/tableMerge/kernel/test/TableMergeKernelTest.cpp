#include "sql/ops/tableMerge/kernel/TableMergeKernel.h"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/builder/GraphDesc.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/Navi.h"
#include "navi/engine/NaviResult.h"
#include "navi/engine/NaviUserResult.h"
#include "navi/engine/ResourceMap.h"
#include "navi/engine/RunGraphParams.h"
#include "navi/log/LoggingEvent.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/test_cluster/NaviTestCluster.h"
#include "navi/util/CommonUtil.h"
#include "navi/util/NaviTestPool.h"
#include "navi/util/ReadyBitMap.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace navi;
using namespace table;
using namespace sql;

namespace sql {

class TableDataSourceKernel : public Kernel {
public:
    TableDataSourceKernel() = default;

public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("TableDataSourceKernel").output("output0", TableType::TYPE_ID);
    }
    navi::ErrorCode compute(KernelComputeContext &ctx) override {
        bool eof = (--_times == 0);
        MatchDocUtil matchDocUtil;
        matchdoc::MatchDocAllocatorPtr allocator;
        const auto &docs = matchDocUtil.createMatchDocs(allocator, 3);
        int32_t base = _times * 2;
        matchDocUtil.extendMatchDocAllocator<int32_t>(
            allocator, docs, "a", {base, base + 1, base + 2});
        if (TESTBASE::HasFatalFailure()) {
            return navi::EC_ABORT;
        }
        auto table = make_shared<Table>(docs, allocator);
        DataPtr data(new TableData(table));
        PortIndex index(0, INVALID_INDEX);
        ctx.setOutput(index, data, eof);
        return navi::EC_NONE;
    }

private:
    size_t _times = 2;
};

class TableDataIdentityKernel : public Kernel {
public:
    TableDataIdentityKernel() = default;

public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("TableDataIdentityKernel")
            .input("input0", TableType::TYPE_ID)
            .output("output0", TableType::TYPE_ID);
    }
    navi::ErrorCode compute(KernelComputeContext &ctx) override {
        navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
        bool isEof = false;
        navi::DataPtr data;
        ctx.getInput(inputIndex, data, isEof);
        ctx.setOutput(inputIndex, data, isEof);
        return navi::EC_NONE;
    }
};

REGISTER_KERNEL(TableDataSourceKernel);
REGISTER_KERNEL(TableDataIdentityKernel);

class TableMergeKernelTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    void initCluster();
    void buildCluster(NaviTestCluster &cluster);
    void buildSimpleGraph(GraphDef *def);

private:
    autil::mem_pool::PoolPtr _poolPtr;
    std::string _loader;
    std::unique_ptr<GraphDef> _graphDef;
    std::unique_ptr<autil::EnvGuard> _naviPythonHome;
};

void TableMergeKernelTest::setUp() {
    _poolPtr.reset(new autil::mem_pool::PoolAsan);
    _naviPythonHome.reset(new autil::EnvGuard(
        "NAVI_PYTHON_HOME", "./aios/navi/config_loader/python:/usr/lib64/python3.6/"));
    _loader = "./aios/navi/testdata/test_config_loader.py";
    _graphDef.reset(new GraphDef());
}

void TableMergeKernelTest::tearDown() {}

void TableMergeKernelTest::buildCluster(NaviTestCluster &cluster) {
    // TableMergeKernel can only be tested by NaviTestCluster
    ResourceMapPtr rootResourceMap(new ResourceMap());
    std::string configPath = "./aios/navi/testdata/config/cluster/";
    std::string biz1Config = "./aios/sql/ops/tableMerge/kernel/testdata/config/biz1.py";

    ASSERT_TRUE(cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_3", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_4", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_5", _loader, configPath, rootResourceMap));

    ASSERT_TRUE(cluster.addBiz("host_0", "biz_qrs", 1, 0, biz1Config));

    ASSERT_TRUE(cluster.addBiz("host_1", "biz_a", 2, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "biz_a", 2, 1, biz1Config));

    ASSERT_TRUE(cluster.addBiz("host_3", "biz_b", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_4", "biz_b", 3, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_5", "biz_b", 3, 2, biz1Config));

    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();
}

void TableMergeKernelTest::buildSimpleGraph(GraphDef *def) {
    GraphBuilder builder(def);
    // a2_source(*2) -- sql.TableMergeKernel --> qrs_identity --> GraphOutput1
    builder.newSubGraph("biz_qrs");
    auto n1 = builder.node("qrs_identity").kernel("TableDataIdentityKernel");
    n1.out("output0").asGraphOutput("GraphOutput1");
    builder.newSubGraph("biz_a");
    builder.node("a2_source")
        .kernel("TableDataSourceKernel")
        .out("output0")
        .to(n1.in("input0"))
        .require(true)
        .merge("sql.TableMergeKernel");
    ASSERT_TRUE(builder.ok());
}

TEST_F(TableMergeKernelTest, testMergeTableData_Error_GetTableFailed) {
    TableMergeKernel kernel;
    DataPtr data;
    NaviLoggerProvider provider;
    ASSERT_FALSE(kernel.mergeTableData(0, data));
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "get table failed for", provider));
}

TEST_F(TableMergeKernelTest, testMergeTableData_EmptyOutputTable) {
    TableMergeKernel kernel;
    kernel._outputTable.reset();
    kernel._dataReadyMap = ReadyBitMap::createReadyBitMap(1);
    kernel._dataReadyMap->setReady(false);

    TablePtr table(new Table(_poolPtr));
    DataPtr data(new TableData(table));
    ASSERT_TRUE(kernel.mergeTableData(0, data));

    ASSERT_EQ(table, kernel._outputTable);
    ASSERT_TRUE(kernel._dataReadyMap->isReady(0));
}

TEST_F(TableMergeKernelTest, testMergeTableData_NonEmptyOutputTable) {
    TableMergeKernel kernel;
    TablePtr oldTable(new Table(_poolPtr));
    kernel._outputTable = oldTable;
    kernel._dataReadyMap = ReadyBitMap::createReadyBitMap(1);
    kernel._dataReadyMap->setReady(false);

    TablePtr table(new Table(_poolPtr));
    DataPtr data(new TableData(table));
    ASSERT_TRUE(kernel.mergeTableData(0, data));

    ASSERT_EQ(oldTable, kernel._outputTable);
    ASSERT_TRUE(kernel._dataReadyMap->isReady(0));
}

TEST_F(TableMergeKernelTest, testSimple) {
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));
    ASSERT_NO_FATAL_FAILURE(buildSimpleGraph(_graphDef.get()));

    RunGraphParams params;
    params.setTimeoutMs(100000);
    // params.setTraceLevel("TRACE3");

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (userResult->nextData(data, eof) && data.data) {
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }

    auto naviResult = userResult->getNaviResult();
    ASSERT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)))
        << naviResult->errorEvent.message;
    ASSERT_EQ("", naviResult->errorEvent.message);
    // naviResult->show();
    ASSERT_EQ(2, dataVec.size());
    {
        auto tableData = dynamic_pointer_cast<TableData>(dataVec[0].data);
        ASSERT_NE(nullptr, tableData);
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(
            tableData->getTable(), "a", {2, 3, 4, 2, 3, 4}));
    }
    {
        auto tableData = dynamic_pointer_cast<TableData>(dataVec[1].data);
        ASSERT_NE(nullptr, tableData);
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(
            tableData->getTable(), "a", {0, 1, 2, 0, 1, 2}));
    }
}

// TEST_F(TableMergeKernelTest, testSimple_PoolUsageExceed)
// {
//     NaviTestCluster cluster;
//     ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));
//     ASSERT_NO_FATAL_FAILURE(buildSimpleGraph(_graphDef.get()));

//     RunGraphParams params;
//     params.setTimeoutMs(100000);
//     // params.setTraceLevel("TRACE3");

//     auto navi = cluster.getNavi("host_0");

//     auto userResult = navi->runGraph(_graphDef.release(), params);

//     std::vector<NaviUserData> dataVec;
//     while (true) {
//         NaviUserData data;
//         bool eof = false;
//         if (userResult->nextData(data, eof) && data.data) {
//             dataVec.push_back(data);
//         }
//         if (eof) {
//             break;
//         }
//     }

//     auto naviResult = userResult->getNaviResult();
//     // naviResult->show();
//     ASSERT_EQ("EC_DESERIALIZE", std::string(CommonUtil::getErrorString(naviResult->ec)));
//     ASSERT_NE("", naviResult->errorEvent.message);
// }

} // namespace sql
