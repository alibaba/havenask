#ifndef WORKER_FRAMEWORK_WORKERBASETEST_H
#define WORKER_FRAMEWORK_WORKERBASETEST_H

#include "worker_framework/WorkerBase.h"

#include "gmock/gmock.h"
#include <algorithm>
#include <sstream>

#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "aios/network/http_arpc/HTTPRPCServer.h"
#include "autil/CommandLineParameter.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fslib.h"
#include "test/test.h"
#include "unittest/unittest.h"
#include "worker_base/test/MockANetRPCServer.h"
#include "worker_base/test/MockHTTPRPCServer.h"
#include "worker_base/test/MockWorkerBase.h"
#include "worker_base/test/TestMessage.pb.h"
#include "worker_framework/PathUtil.h"

using namespace arpc;
using namespace http_arpc;
using namespace std;
using namespace fslib;
using namespace fslib::fs;
using ::testing::_;
using ::testing::DoAll;
using ::testing::IgnoreResult;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;

using namespace worker_framework;
namespace worker_framework {
class WorkerBaseImpl;
class AdapterTestImpl : public worker_framework::Test {
public:
    void QueryPerson(::google::protobuf::RpcController *controller,
                     const Query *request,
                     PersonInfo *response,
                     RPCClosure *done) {}
};

class WorkerBaseTest : public ::testing::Test {
protected:
    WorkerBaseTest() {}

    virtual ~WorkerBaseTest() {}

    virtual void SetUp() {
        _testDataPath = getTestDataPath();
        string defaultLogConfFile = _testDataPath + "/" + "worker_framework_alog.conf";
        FileSystem::remove(defaultLogConfFile);
    }

    virtual void TearDown() {}

protected:
    string _testDataPath;
};

TEST_F(WorkerBaseTest, testInitHTTPRPCServerAfterRegisterService) {
    arpc::ThreadPoolDescriptor tpd = arpc::ThreadPoolDescriptor("test_service", 1, 32);
    WorkerBase worker;
    worker.initRPCServer();
    AdapterTestImpl test;
    worker.registerService(&test, tpd);
    worker.initHTTPRPCServer();
    HTTPRPCServer *httpRPCServer = worker.getHTTPRPCServer();
    vector<string> testMap = httpRPCServer->getRPCNames();
    EXPECT_EQ(uint(1), testMap.size());
}

TEST_F(WorkerBaseTest, testInitHTTPRPCServerBeforeRegisterService) {
    arpc::ThreadPoolDescriptor tpd = arpc::ThreadPoolDescriptor("test_service", 1, 32);
    WorkerBase worker;
    worker.initRPCServer();
    AdapterTestImpl test;
    worker.initHTTPRPCServer();
    worker.registerService(&test, tpd);
    HTTPRPCServer *httpRPCServer = worker.getHTTPRPCServer();
    vector<string> testMap = httpRPCServer->getRPCNames();
    EXPECT_EQ(uint(0), testMap.size());
}

TEST_F(WorkerBaseTest, testInit) {
    MockWorkerBase worker;
    MockANetRPCServer *rpcServer = new MockANetRPCServer;
    MockHTTPRPCServer *httpRPCServer = new MockHTTPRPCServer(rpcServer);
    {
        InSequence dummy;
        EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
        EXPECT_CALL(worker, doExtractOptions()).WillOnce(Return());
        EXPECT_CALL(worker, doInit())
            .WillOnce(DoAll(Invoke(std::bind(&WorkerBase::setRPCServer, &worker, rpcServer)),
                            Invoke(std::bind(&WorkerBase::setHTTPRPCServer, &worker, httpRPCServer)),
                            Return(true)));
        EXPECT_CALL(*rpcServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*httpRPCServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*rpcServer, Close()).WillRepeatedly(Return(true));
        EXPECT_CALL(*httpRPCServer, Close()).WillRepeatedly(Return());
    }
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr =
        "./WorkerBase -p 1234 -l " + logConfFile + " -w " + _testDataPath + " -g 7777" + " --httpPort 4321";

    autil::CommandLineParameter cmdLinePara(commandStr);
    EXPECT_TRUE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
    EXPECT_EQ((uint32_t)1234, worker.getPort());
    EXPECT_EQ((uint32_t)4321, worker.getHTTPPort());
    EXPECT_EQ(logConfFile, worker.getLogConfFile());
    EXPECT_FALSE(worker.getIsRecommendPort());

    std::string retPath;
    ASSERT_TRUE(PathUtil::getCurrentPath(retPath));
    EXPECT_EQ(retPath, worker.getCwdPath());
    worker.setRPCServer(NULL);
    worker.setHTTPRPCServer(NULL);
}

TEST_F(WorkerBaseTest, testInitWithDefaultArgsHasDefaultLoggerConf) {
    MockWorkerBase worker;
    MockANetRPCServer *rpcServer = new MockANetRPCServer;
    MockHTTPRPCServer *httpRPCServer = new MockHTTPRPCServer(rpcServer);
    {
        InSequence dummy;
        EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
        EXPECT_CALL(worker, doExtractOptions()).WillOnce(Return());
        EXPECT_CALL(worker, doInit())
            .WillOnce(DoAll(Invoke(std::bind(&WorkerBase::setRPCServer, &worker, rpcServer)),
                            Invoke(std::bind(&WorkerBase::setHTTPRPCServer, &worker, httpRPCServer)),
                            Return(true)));
        EXPECT_CALL(*rpcServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*rpcServer, Close()).WillRepeatedly(Return(true));
        EXPECT_CALL(*httpRPCServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*httpRPCServer, Close()).WillRepeatedly(Return());
    }
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string defaultLogConfFile = PathUtil::joinPath(_testDataPath, "worker_framework_alog.conf");
    FileSystem::copy(logConfFile, defaultLogConfFile);

    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr = "./WorkerBase -p 1234 -z " + zkPath + " -w " + _testDataPath + " --httpPort 4321";
    autil::CommandLineParameter cmdLinePara(commandStr);
    EXPECT_TRUE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
    EXPECT_EQ((uint32_t)1234, worker.getPort());
    EXPECT_EQ((uint32_t)4321, worker.getHTTPPort());
    EXPECT_EQ("worker_framework_alog.conf", worker.getLogConfFile());
    EXPECT_FALSE(worker.getIsRecommendPort());
    std::string retPath;
    ASSERT_TRUE(PathUtil::getCurrentPath(retPath));
    EXPECT_EQ(retPath, worker.getCwdPath());
    worker.setRPCServer(NULL);
    worker.setHTTPRPCServer(NULL);
}

TEST_F(WorkerBaseTest, testInitWithDefaultArgs) {
    MockWorkerBase worker;
    MockANetRPCServer *rpcServer = new MockANetRPCServer;
    MockHTTPRPCServer *httpRPCServer = new MockHTTPRPCServer(rpcServer);
    {
        InSequence dummy;
        EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
        EXPECT_CALL(worker, doExtractOptions()).WillOnce(Return());
        EXPECT_CALL(worker, doInit())
            .WillOnce(DoAll(Invoke(std::bind(&WorkerBase::setRPCServer, &worker, rpcServer)),
                            Invoke(std::bind(&WorkerBase::setHTTPRPCServer, &worker, httpRPCServer)),
                            Return(true)));
        EXPECT_CALL(*rpcServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*rpcServer, Close()).WillRepeatedly(Return(true));
        EXPECT_CALL(*httpRPCServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*httpRPCServer, Close()).WillRepeatedly(Return());
    }
    string logConfFile = "";
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr = "./WorkerBase -p 1234 -z " + zkPath + " --httpPort 4321";
    autil::CommandLineParameter cmdLinePara(commandStr);
    EXPECT_TRUE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
    EXPECT_EQ((uint32_t)1234, worker.getPort());
    EXPECT_EQ((uint32_t)4321, worker.getHTTPPort());
    EXPECT_EQ(logConfFile, worker.getLogConfFile());
    EXPECT_FALSE(worker.getIsRecommendPort());
    std::string retPath;
    ASSERT_TRUE(PathUtil::getCurrentPath(retPath));
    EXPECT_EQ(retPath, worker.getCwdPath());
    worker.setRPCServer(NULL);
    worker.setHTTPRPCServer(NULL);
}

TEST_F(WorkerBaseTest, testInitFailParseArgs) {
    MockWorkerBase worker;
    {
        InSequence dummy;
        EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
        EXPECT_CALL(worker, doExtractOptions()).Times(0);
        EXPECT_CALL(worker, doInit()).Times(0);
    }
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr = "./WorkerBase -p 1234a -z " + zkPath;
    autil::CommandLineParameter cmdLinePara(commandStr);
    EXPECT_FALSE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
}

TEST_F(WorkerBaseTest, testInitFailDoInit) {
    MockWorkerBase worker;
    EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
    {
        InSequence dummy;
        EXPECT_CALL(worker, doExtractOptions()).WillOnce(Return());
        EXPECT_CALL(worker, doInit()).WillOnce(Return(false));
    }
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr = "./WorkerBase -p 1234 -z " + zkPath;
    autil::CommandLineParameter cmdLinePara(commandStr);
    EXPECT_FALSE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
}

bool killSignal() {
    kill(getpid(), SIGUSR1);
    return true;
}

TEST_F(WorkerBaseTest, testRun) {
    MockWorkerBase worker;
    MockANetRPCServer *rpcServer = new MockANetRPCServer;
    MockHTTPRPCServer *httpRPCServer = new MockHTTPRPCServer(rpcServer);
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr =
        "./WorkerBase -p 1234 -l " + logConfFile + " -w " + _testDataPath + " -g 7777 -z " + zkPath;
    autil::CommandLineParameter cmdLinePara(commandStr);
    EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
    EXPECT_CALL(worker, doExtractOptions()).WillOnce(Return());
    EXPECT_CALL(*rpcServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
    EXPECT_CALL(*rpcServer, Close()).WillRepeatedly(Return(true));
    EXPECT_CALL(*httpRPCServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
    EXPECT_CALL(*httpRPCServer, Close()).WillRepeatedly(Return());
    EXPECT_CALL(worker, doInit())
        .WillOnce(DoAll(Invoke(std::bind(&WorkerBase::setRPCServer, &worker, rpcServer)),
                        Invoke(std::bind(&WorkerBase::setHTTPRPCServer, &worker, httpRPCServer)),
                        Return(true)));
    EXPECT_CALL(worker, doStart()).WillOnce(DoAll(IgnoreResult(Invoke(killSignal)), Return(true)));
    EXPECT_CALL(worker, doStop()).WillOnce(Return());
    EXPECT_TRUE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
    EXPECT_TRUE(worker.run());
}

TEST_F(WorkerBaseTest, testRunDoStartFailed) {
    MockWorkerBase worker;
    MockANetRPCServer *rpcServer = new MockANetRPCServer;
    MockHTTPRPCServer *httpRPCServer = new MockHTTPRPCServer(rpcServer);
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr =
        "./WorkerBase -p 1234 -l " + logConfFile + " -w " + _testDataPath + " -g 7777 -z " + zkPath;
    autil::CommandLineParameter cmdLinePara(commandStr);
    EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
    EXPECT_CALL(worker, doExtractOptions()).WillOnce(Return());
    EXPECT_CALL(worker, doInit())
        .WillOnce(DoAll(Invoke(std::bind(&WorkerBase::setRPCServer, &worker, rpcServer)),
                        Invoke(std::bind(&WorkerBase::setHTTPRPCServer, &worker, httpRPCServer)),
                        Return(true)));
    EXPECT_CALL(*rpcServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
    EXPECT_CALL(*rpcServer, Close()).WillRepeatedly(Return(true));
    EXPECT_CALL(*httpRPCServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
    EXPECT_CALL(*httpRPCServer, Close()).WillRepeatedly(Return());
    EXPECT_CALL(worker, doStart()).WillOnce(DoAll(IgnoreResult(Invoke(killSignal)), Return(false)));
    EXPECT_CALL(worker, doStop()).Times(1);

    EXPECT_TRUE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
    EXPECT_FALSE(worker.run());
}

TEST_F(WorkerBaseTest, testAPRCNormal) {
    MockWorkerBase worker;
    MockANetRPCServer *rpcServer = new MockANetRPCServer;
    MockHTTPRPCServer *httpRPCServer = new MockHTTPRPCServer(rpcServer);
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr =
        "./WorkerBase -p 112 --httpPort 113 -l " + logConfFile + " -w " + _testDataPath + " -g 7777 -z " + zkPath;
    autil::CommandLineParameter cmdLinePara(commandStr);

    EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
    EXPECT_CALL(worker, doExtractOptions()).WillOnce(Return());

    EXPECT_CALL(worker, doInit())
        .WillOnce(DoAll(Invoke(std::bind(&WorkerBase::setRPCServer, &worker, rpcServer)),
                        Invoke(std::bind(&WorkerBase::setHTTPRPCServer, &worker, httpRPCServer)),
                        Return(true)));
    EXPECT_CALL(*rpcServer, Listen(_, _, _, _)).WillRepeatedly(Return(false));
    EXPECT_CALL(*rpcServer, Listen(std::string("tcp:0.0.0.0:112"), _, _, _)).WillOnce(Return(true));
    EXPECT_CALL(*rpcServer, Close()).WillRepeatedly(Return(true));
    EXPECT_CALL(*httpRPCServer, Listen(_, _, _, _)).WillRepeatedly(Return(false));
    EXPECT_CALL(*httpRPCServer, Listen(std::string("tcp:0.0.0.0:113"), _, _, _)).WillOnce(Return(true));
    EXPECT_CALL(*httpRPCServer, Close()).WillRepeatedly(Return());
    EXPECT_TRUE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
    worker.setRPCServer(NULL);
    worker.setHTTPRPCServer(NULL);
}

TEST_F(WorkerBaseTest, testAPRCFailed) {
    MockWorkerBase worker;
    MockANetRPCServer *rpcServer = new MockANetRPCServer;
    MockHTTPRPCServer *httpRPCServer = new MockHTTPRPCServer(rpcServer);
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr =
        "./WorkerBase -p 112 -l " + logConfFile + " -w " + _testDataPath + " -g 7777 -z " + zkPath;
    autil::CommandLineParameter cmdLinePara(commandStr);

    EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
    EXPECT_CALL(worker, doExtractOptions()).WillOnce(Return());

    EXPECT_CALL(worker, doInit())
        .WillOnce(DoAll(Invoke(std::bind(&WorkerBase::setRPCServer, &worker, rpcServer)),
                        Invoke(std::bind(&WorkerBase::setHTTPRPCServer, &worker, httpRPCServer)),
                        Return(true)));
    EXPECT_CALL(*rpcServer, Listen(_, _, _, _)).WillRepeatedly(Return(false));
    EXPECT_CALL(*rpcServer, Close()).WillRepeatedly(Return(true));
    EXPECT_CALL(*httpRPCServer, Listen(_, _, _, _)).WillRepeatedly(Return(false));
    EXPECT_CALL(*httpRPCServer, Close()).WillRepeatedly(Return());
    EXPECT_FALSE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
}

TEST_F(WorkerBaseTest, testAPRCNormalWithRandomPort) {
    MockWorkerBase worker;
    MockANetRPCServer *rpcServer = new MockANetRPCServer;
    MockHTTPRPCServer *httpRPCServer = new MockHTTPRPCServer(rpcServer);
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr = "./WorkerBase -l " + logConfFile + " -w " + _testDataPath + " -g 7777 -z " + zkPath;
    autil::CommandLineParameter cmdLinePara(commandStr);

    EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
    EXPECT_CALL(worker, doExtractOptions()).WillOnce(Return());

    EXPECT_CALL(worker, doInit())
        .WillOnce(DoAll(Invoke(std::bind(&WorkerBase::setRPCServer, &worker, rpcServer)),
                        Invoke(std::bind(&WorkerBase::setHTTPRPCServer, &worker, httpRPCServer)),
                        Return(true)));
    EXPECT_CALL(*rpcServer, Listen(_, _, _, _)).WillOnce(Return(false)).WillOnce(Return(true));
    EXPECT_CALL(*httpRPCServer, Listen(_, _, _, _)).WillOnce(Return(false)).WillOnce(Return(true));

    EXPECT_CALL(*rpcServer, Close()).WillRepeatedly(Return(true));
    EXPECT_CALL(*httpRPCServer, Close()).WillRepeatedly(Return());

    EXPECT_TRUE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
    ASSERT_TRUE(worker.getPort() > 0);
}

TEST_F(WorkerBaseTest, testInitWithRecommendPort) {
    MockWorkerBase worker;
    MockANetRPCServer *rpcServer = new MockANetRPCServer;
    MockHTTPRPCServer *httpRPCServer = new MockHTTPRPCServer(rpcServer);
    {
        InSequence dummy;
        EXPECT_CALL(worker, doInitOptions()).WillOnce(Return());
        EXPECT_CALL(worker, doExtractOptions()).WillOnce(Return());
        EXPECT_CALL(worker, doInit())
            .WillOnce(DoAll(Invoke(std::bind(&WorkerBase::setRPCServer, &worker, rpcServer)),
                            Invoke(std::bind(&WorkerBase::setHTTPRPCServer, &worker, httpRPCServer)),
                            Return(true)));
        EXPECT_CALL(*rpcServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*httpRPCServer, Listen(_, _, _, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*rpcServer, Close()).WillRepeatedly(Return(true));
        EXPECT_CALL(*httpRPCServer, Close()).WillRepeatedly(Return());
    }
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr =
        "./WorkerBase -p 0 -l " + logConfFile + " -w " + _testDataPath + " -g 7777" + " --httpPort 0 --recommendPort";

    autil::CommandLineParameter cmdLinePara(commandStr);
    EXPECT_TRUE(worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv()));
    EXPECT_EQ(logConfFile, worker.getLogConfFile());
    EXPECT_TRUE(worker.getIsRecommendPort());

    std::string retPath;
    ASSERT_TRUE(PathUtil::getCurrentPath(retPath));
    EXPECT_EQ(retPath, worker.getCwdPath());
    worker.setRPCServer(NULL);
    worker.setHTTPRPCServer(NULL);
}

TEST_F(WorkerBaseTest, testEnableANetMetric) {
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr = "./WorkerBase -p 112 --httpPort 113 -l " + logConfFile + " -w " + _testDataPath +
                              " -g 7777 -z " + zkPath + " --enableANetMetric --enableARPCMetric";
    autil::CommandLineParameter cmdLinePara(commandStr);
    WorkerBase worker;
    worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv());
    auto *rpcServer = worker.getRPCServer();
    ASSERT_TRUE(rpcServer != nullptr);
    ASSERT_TRUE(rpcServer->_metricReporter != nullptr);
}

TEST_F(WorkerBaseTest, testDisableANetMetric) {
    string logConfFile = _testDataPath + "/WorkerBaseTestLogger.conf";
    string zkPath = "zfs://1.2.3.4:1234/test";
    const string commandStr =
        "./WorkerBase -p 112 --httpPort 113 -l " + logConfFile + " -w " + _testDataPath + " -g 7777 -z " + zkPath;
    autil::CommandLineParameter cmdLinePara(commandStr);
    WorkerBase worker;
    worker.init(cmdLinePara.getArgc(), cmdLinePara.getArgv());
    auto *rpcServer = worker.getRPCServer();
    ASSERT_TRUE(rpcServer != nullptr);
    ASSERT_TRUE(rpcServer->_metricReporter == nullptr);
}

}; // namespace worker_framework

#endif // WORKER_FRAMEWORK_WORKERBASETEST_H
