#include <memory>

#include "TestPortUtil.h"
#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "aios/network/gig/multi_call/agent/GigAgent.h"
#include "aios/network/gig/multi_call/rpc/GigRpcServer.h"
#include "autil/RangeUtil.h"
#include "autil/StringUtil.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/mock/MockTablet.h"
#include "indexlib/framework/mock/MockTabletReader.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "suez/common/test/TableMetaUtil.h"
#include "suez/sdk/RpcServer.h"
#include "suez/sdk/SuezTabletPartitionData.h"
#include "suez/service/TableServiceImpl.h"
#include "table/TableUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace table;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TableServiceTest);

class TableServiceTest : public TESTBASE {
public:
    void setUp() {
        AUTIL_ROOT_LOG_SETLEVEL(INFO);
        _executor = make_unique<future_lite::executors::SimpleExecutor>(5);
        _rpcServer = make_unique<multi_call::GigRpcServer>();
    }

    SearchInitParam createSearchInitParam() {
        SearchInitParam param;
        KMonitorMetaInfo kmonMetaInfo;
        kmonMetaInfo.metricsPrefix = "suez.table_service_test";
        param.kmonMetaInfo = kmonMetaInfo;
        param.asyncIntraExecutor = _executor.get();
        _rpcServerWrapper.arpcServer = _rpcServer->getArpcServer();
        _rpcServerWrapper.httpRpcServer = _rpcServer->getHttpArpcServer();
        _rpcServerWrapper.gigRpcServer = _rpcServer.get();
        param.rpcServer = &_rpcServerWrapper;
        return param;
    }

    bool startGigRpcServer() {
        if (!initGrpcServer()) {
            return false;
        }
        auto gigAgent = _rpcServer->getAgent();
        if (!gigAgent) {
            return false;
        }
        return gigAgent->init();
    }

    bool initGrpcServer();
    std::shared_ptr<indexlibv2::framework::ITablet> createTablet(const string &tableName);
    std::shared_ptr<IndexProvider> createIndexProvider(const string &tableName, int partCnt);

private:
    std::unique_ptr<indexlibv2::table::KVTableTestHelper> _helper;
    std::shared_ptr<multi_call::GigRpcServer> _rpcServer;
    RpcServer _rpcServerWrapper;
    std::unique_ptr<future_lite::Executor> _executor;
};

bool TableServiceTest::initGrpcServer() {
    vector<int> ports;
    if (!TestPortUtil::SelectUnusedPorts(ports, 3)) {
        AUTIL_LOG(ERROR, "failed to get unused ports");
        return false;
    }
    string gigGrpcPort = autil::StringUtil::toString(ports[0]);
    string arpcPort = autil::StringUtil::toString(ports[1]);
    string httpPort = autil::StringUtil::toString(ports[2]);
    AUTIL_LOG(INFO, "get ports [%d] [%d] [%d]", ports[0], ports[1], ports[2]);

    multi_call::GrpcServerDescription grpcDesc;
    grpcDesc.ioThreadNum = 2;
    grpcDesc.port = gigGrpcPort;
    if (!_rpcServer->initGrpcServer(grpcDesc)) {
        AUTIL_LOG(ERROR, "initGrpcServer failed.");
        return false;
    }
    multi_call::ArpcServerDescription arpcDesc;
    arpcDesc.port = arpcPort;
    if (!_rpcServer->startArpcServer(arpcDesc)) {
        AUTIL_LOG(ERROR, "gig init ArpcServer failed.");
        return false;
    }
    multi_call::HttpArpcServerDescription httpDesc;
    httpDesc.port = httpPort;
    if (!_rpcServer->startHttpArpcServer(httpDesc)) {
        AUTIL_LOG(ERROR, "gig init HttpServer failed.");
        return false;
    }

    AUTIL_LOG(INFO, "initGrpcServer success, grpc listen port [%d]", _rpcServer->getGrpcPort());
    return true;
}

std::shared_ptr<indexlibv2::framework::ITablet> TableServiceTest::createTablet(const string &tableName) {
    auto schemaPtr = indexlibv2::table::KVTabletSchemaMaker::Make(
        "pk:string;int_field:int32;string_field:string;multi_string_field:string:true",
        "pk",
        "int_field;string_field;multi_string_field");
    schemaPtr->TEST_SetTableName(tableName);
    string docStr = "cmd=add,pk=pk0,"
                    "int_field=2,"
                    "string_field=str0,"
                    "multi_string_field=str00 str01 str02 str03,"
                    "cmd=add,pk=pk1,"
                    "int_field=4,"
                    "string_field=str1,"
                    "multi_string_field=str10 str11 str12 str13;";
    const string path = GET_TEMP_DATA_PATH() + "/kv_" + tableName;
    indexlibv2::framework::IndexRoot indexRoot(path, path);
    unique_ptr<indexlibv2::config::TabletOptions> tabletOptions = make_unique<indexlibv2::config::TabletOptions>();
    tabletOptions->SetIsOnline(true);
    _helper = std::make_unique<indexlibv2::table::KVTableTestHelper>();
    auto status = _helper->Open(indexRoot, schemaPtr, move(tabletOptions));
    if (!status.IsOK()) {
        return nullptr;
    }
    auto tablet = _helper->GetITablet();
    status = _helper->Build(docStr);
    if (!status.IsOK()) {
        return nullptr;
    }
    return tablet;
}

std::shared_ptr<IndexProvider> TableServiceTest::createIndexProvider(const string &tableName, int partCnt) {
    auto provider = make_shared<IndexProvider>();
    MultiTableReader multiReader;
    auto rangeVec = autil::RangeUtil::splitRange(0, 65535, partCnt);
    for (int idx = 0; idx < partCnt; idx++) {
        PartitionId pid;
        pid.tableId.fullVersion = 1;
        pid.tableId.tableName = tableName;
        pid.partCount = partCnt;
        pid.from = rangeVec[idx].first;
        pid.to = rangeVec[idx].second;
        pid.index = idx;
        auto tablet = createTablet(tableName);
        SuezPartitionDataPtr partitionData = std::make_shared<SuezTabletPartitionData>(pid, partCnt, tablet, true);
        SingleTableReaderPtr singleReader(new SingleTableReader(partitionData));
        multiReader.addTableReader(pid, singleReader);
    }
    provider->multiTableReader = multiReader;
    return provider;
}

TEST_F(TableServiceTest, testInit) {
    ASSERT_TRUE(startGigRpcServer());
    TableServiceImpl service;
    auto param = createSearchInitParam();
    ASSERT_TRUE(service.init(param));
}

TEST_F(TableServiceTest, testNeedUpdateTopoInfo) {
    ASSERT_TRUE(startGigRpcServer());
    TableServiceImpl service;
    auto param = createSearchInitParam();
    ASSERT_TRUE(service.init(param));

    auto indexProvider = createIndexProvider("test_table_1_part", 1);
    UpdateArgs updateArgs;
    updateArgs.indexProvider = indexProvider;
    ASSERT_TRUE(service.needUpdateTopoInfo(updateArgs));
    UpdateIndications updateIndications;
    SuezErrorCollector errorCollector;
    auto ret = service.update(updateArgs, updateIndications, errorCollector);
    ASSERT_EQ(UR_REACH_TARGET, ret);
    ASSERT_FALSE(service.needUpdateTopoInfo(updateArgs));
}

TEST_F(TableServiceTest, testBuildTableTopoInfo) {
    ASSERT_TRUE(startGigRpcServer());
    TableServiceImpl service;
    auto param = createSearchInitParam();
    ASSERT_TRUE(service.init(param));

    string tableName = "test_table_2_parts";
    int partCnt = 2;
    string zoneName = "zone1";
    auto indexProvider = createIndexProvider(tableName, partCnt);
    service.setIndexProvider(indexProvider);
    ServiceInfo serviceInfo;
    serviceInfo._zoneName = zoneName;
    UpdateArgs updateArgs;
    updateArgs.indexProvider = indexProvider;
    updateArgs.serviceInfo = serviceInfo;
    multi_call::TopoInfoBuilder infoBuilder;
    service.buildTableTopoInfo(&_rpcServerWrapper, updateArgs, infoBuilder);
    auto topoVec = infoBuilder.getBizTopoInfo();
    ASSERT_EQ(partCnt, topoVec.size());
    for (auto idx = 0; idx < partCnt; idx++) {
        const auto &topo = topoVec[idx];
        ASSERT_EQ(zoneName + ".table_service." + tableName, topo.bizName);
        ASSERT_EQ(partCnt, topo.partCnt);
        ASSERT_EQ(idx, topo.partId);
        ASSERT_EQ(_rpcServer->getGrpcPort(), topo.gigRpcPort);
    }
}

TEST_F(TableServiceTest, testUpdateTableTopoInfo) {
    ASSERT_TRUE(startGigRpcServer());
    TableServiceImpl service;
    auto param = createSearchInitParam();
    ASSERT_TRUE(service.init(param));
    string tableName = "test_table_2_parts";
    int partCnt = 2;
    auto indexProvider = createIndexProvider(tableName, partCnt);
    service.setIndexProvider(indexProvider);
    ServiceInfo serviceInfo;
    serviceInfo._zoneName = "zone1";
    UpdateArgs updateArgs;
    updateArgs.indexProvider = indexProvider;
    updateArgs.serviceInfo = serviceInfo;
    ASSERT_TRUE(service.updateTopoInfo(updateArgs));

    // will not add dup table topo info
    ASSERT_TRUE(service.updateTopoInfo(updateArgs));
}

TEST_F(TableServiceTest, testUpdateIndexProvider) {
    ASSERT_TRUE(startGigRpcServer());
    TableServiceImpl service;
    auto param = createSearchInitParam();
    ASSERT_TRUE(service.init(param));

    auto indexProvider = createIndexProvider("test_table_1_part", 1);
    UpdateArgs updateArgs;
    updateArgs.indexProvider = indexProvider;
    ASSERT_TRUE(service.needUpdateTopoInfo(updateArgs));
    UpdateIndications updateIndications;
    SuezErrorCollector errorCollector;
    auto ret0 = service.update(updateArgs, updateIndications, errorCollector);
    ASSERT_EQ(UR_REACH_TARGET, ret0);
    ASSERT_EQ(3, indexProvider.use_count());

    updateArgs.indexProvider = std::make_shared<IndexProvider>();
    *updateArgs.indexProvider = *indexProvider;
    ASSERT_FALSE(service.needUpdateTopoInfo(updateArgs));
    auto ret1 = service.update(updateArgs, updateIndications, errorCollector);
    ASSERT_EQ(UR_REACH_TARGET, ret1);
    ASSERT_EQ(1, indexProvider.use_count());
}

TEST_F(TableServiceTest, testSimpleQuery) {
    PartitionId pid1 = TableMetaUtil::makePidFromStr("mock_1");
    pid1.setPartitionIndex(1);
    auto tablet = std::make_shared<indexlibv2::framework::NiceMockTablet>();
    auto tabletReader = std::make_shared<indexlibv2::framework::MockTabletReader>();
    EXPECT_CALL(*tablet, GetTabletReader())
        .Times(2)
        .WillRepeatedly(Return(std::dynamic_pointer_cast<indexlibv2::framework::ITabletReader>(tabletReader)));
    EXPECT_CALL(*tabletReader, Search(_, _))
        .WillOnce(Invoke([](const std::string &query, std::string &result) -> indexlib::Status {
            return indexlib::Status::InvalidArgs();
        }))
        .WillOnce(Invoke([](const std::string &query, std::string &result) -> indexlib::Status {
            result = "test";
            return indexlib::Status::OK();
        }));
    auto data1 = std::make_shared<SuezTabletPartitionData>(pid1, 1, tablet, false);
    auto singleReader1 = std::make_shared<SingleTableReader>(data1);
    MultiTableReader mutliTableReader1;
    mutliTableReader1.addTableReader(pid1, singleReader1);
    IndexProvider indexProvider;
    indexProvider.multiTableReader = mutliTableReader1;

    TableServiceImpl impl;
    impl.setIndexProvider(std::make_shared<IndexProvider>(indexProvider));

    SimpleQueryRequest request1;
    request1.set_table("mock");
    SimpleQueryResponse response1;
    impl.simpleQuery(nullptr, &request1, &response1, nullptr);
    ASSERT_EQ(TBS_ERROR_GET_FAILED, response1.errorinfo().errorcode());

    SimpleQueryRequest request2;
    request2.set_table("mock");
    SimpleQueryResponse response2;
    impl.simpleQuery(nullptr, &request2, &response2, nullptr);
    ASSERT_EQ(TBS_ERROR_NONE, response2.errorinfo().errorcode());
    ASSERT_EQ("test", response2.response());
}

} // namespace suez
