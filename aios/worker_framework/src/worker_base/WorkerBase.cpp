/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "worker_framework/WorkerBase.h"

#include <signal.h>
#include <sys/stat.h>

#include "aios/network/http_arpc/HTTPRPCServer.h"
#include "autil/EnvUtil.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/NetUtil.h"
#include "autil/StringUtil.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fslib.h"
#include "worker_framework/PathUtil.h"
#include "fslib/util/FileUtil.h"
#include <sys/stat.h>


using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace fslib::util;
using namespace autil;

using namespace worker_framework;
namespace worker_framework {

AUTIL_DECLARE_AND_SETUP_LOGGER(worker_framework.worker_base, WorkerBase);

#define MIN_RANDOM_PORT 50000
#define MAX_RANDOM_PORT 60000
#define RPC_SERVER_THREAD_NUM 16
#define RPC_SERVER_IO_THREAD_NUM 1
#define RPC_SERVER_THREAD_POOL_QUEUE_SIZE 500
#define IDLE_SLEEP_TIME_US 100000

volatile static bool isStop = false;
static sighandler_t __func_int = nullptr;
static sighandler_t __func_term = nullptr;
static sighandler_t __func_user1 = nullptr;
static sighandler_t __func_user2 = nullptr;

static void handleSignal(int sig) {
    // avoid log after global dtor
    if (!isStop) {
        AUTIL_LOG(INFO, "receive signal [%d], stop", sig);
    }
    isStop = true;
}

static void registerSignalHandler() {
    __func_int = signal(SIGINT, handleSignal);
    __func_term = signal(SIGTERM, handleSignal);
    __func_user1 = signal(SIGUSR1, handleSignal);
    __func_user2 = signal(SIGUSR2, handleSignal);
}

static void resetSignalHandler() {
    if (__func_int != nullptr) signal(SIGINT, __func_int);
    if (__func_term != nullptr) signal(SIGTERM, __func_term);
    if (__func_user1 != nullptr) signal(SIGUSR1, __func_user1);
    if (__func_user2 != nullptr) signal(SIGUSR2, __func_user2);
}

class WorkerBaseImpl {
public:
    WorkerBaseImpl()
        : transport(NULL)
        , rpcServer(NULL)
        , httpRPCServer(NULL)
        , threadNum(RPC_SERVER_THREAD_NUM)
        , ioThreadNum(RPC_SERVER_IO_THREAD_NUM)
        , queueSize(RPC_SERVER_THREAD_POOL_QUEUE_SIZE)
        , port(0)
        , httpPort(0)
        , anetTimeoutLoopIntervalMs(0)
        , exclusiveListenThread(false)
        , isRpcServerStarted(false)
        , deamon(false)
        , reInit(false)
        , isStopped(false)
        , recommendPort(false)
        , idleSleepTimeUs(IDLE_SLEEP_TIME_US) {}
    ~WorkerBaseImpl() {}

    std::string logConfFile;
    std::string cwdPath;
    autil::ThreadMutex mutex;
    anet::Transport *transport;
    arpc::ANetRPCServer *rpcServer;
    http_arpc::HTTPRPCServer *httpRPCServer;
    autil::OptionParser optionParser;
    uint32_t threadNum;
    uint32_t ioThreadNum;
    uint32_t queueSize;
    int32_t port;
    int32_t httpPort;
    int32_t anetTimeoutLoopIntervalMs;
    bool exclusiveListenThread;
    std::string ldLibraryPath;
    bool isRpcServerStarted;
    bool deamon;
    bool reInit;
    bool isStopped;
    bool recommendPort;
    uint32_t idleSleepTimeUs;
};

WorkerBase::WorkerBase() : _impl(new WorkerBaseImpl) {}

WorkerBase::~WorkerBase() {
    stopRPCServer();
    DELETE_AND_SET_NULL(_impl);
    resetSignalHandler();
    AUTIL_LOG(INFO, "deconstruct worker base end");
}

autil::OptionParser &WorkerBase::getOptionParser() { return _impl->optionParser; }

const autil::OptionParser &WorkerBase::getOptionParser() const { return _impl->optionParser; }

string WorkerBase::getIp() const { return autil::NetUtil::getBindIp(); }

uint32_t WorkerBase::getPort() const { return _impl->port; }

void WorkerBase::setPort(uint32_t port) { _impl->port = port; }

string WorkerBase::getAddress() const { return getIp() + ":" + autil::StringUtil::toString(getPort()); }

uint32_t WorkerBase::getHTTPPort() const { return _impl->httpPort; }

void WorkerBase::setHTTPPort(uint32_t httpPort) { _impl->httpPort = httpPort; }

string WorkerBase::getHTTPAddress() const { return getIp() + ":" + autil::StringUtil::toString(getHTTPPort()); }

bool WorkerBase::supportHTTP() const { return _impl->httpRPCServer != NULL; }

string WorkerBase::getCwdPath() const { return _impl->cwdPath; }

string WorkerBase::getVersion() { return ""; }

string WorkerBase::getLogConfFile() const { return _impl->logConfFile; }

void WorkerBase::setReInit() { _impl->reInit = true; }

bool WorkerBase::isStopped() { return _impl->isStopped; }

void WorkerBase::stop() { _impl->isStopped = true; }

bool WorkerBase::getIsRecommendPort() const { return _impl->recommendPort; }

arpc::ANetRPCServer *WorkerBase::getRPCServer() {
    if (_impl) {
        return _impl->rpcServer;
    }
    return NULL;
}

void WorkerBase::setRPCServer(arpc::ANetRPCServer *rpcServer) {
    if (_impl->rpcServer) {
        delete _impl->rpcServer;
    }
    _impl->rpcServer = rpcServer;
}

void WorkerBase::setHTTPRPCServer(http_arpc::HTTPRPCServer *httpRPCServer) {
    if (_impl->httpRPCServer) {
        delete _impl->httpRPCServer;
    }
    _impl->httpRPCServer = httpRPCServer;
}

http_arpc::HTTPRPCServer *WorkerBase::getHTTPRPCServer() { return _impl->httpRPCServer; }

void WorkerBase::initOptions() {
    getOptionParser().addOption("-p", "--port", "port", (int32_t)0);
    getOptionParser().addOption("", "--httpPort", "httpPort", (int32_t)0);
    getOptionParser().addOption("-l", "--logConf", "logConf", OptionParser::OPT_STRING, false);
    getOptionParser().addOption("-w", "--workDir", "workDir", OptionParser::OPT_STRING, false);
    getOptionParser().addOption("", "--deamon", "deamon", OptionParser::STORE_TRUE);
    getOptionParser().addOption("-t", "--threadNum", "threadNum", RPC_SERVER_THREAD_NUM);
    getOptionParser().addOption("-q", "--queueSize", "queueSize", RPC_SERVER_THREAD_POOL_QUEUE_SIZE);
    getOptionParser().addOption("-i", "--ioThreadNum", "ioThreadNum", RPC_SERVER_IO_THREAD_NUM);
    getOptionParser().addOption("", "--anetTimeoutLoopIntervalMs", "anetTimeoutLoopIntervalMs", (int32_t)0);
    getOptionParser().addOption("", "--recommendPort", "recommendPort", OptionParser::STORE_TRUE);
    getOptionParser().addOption("", "--exclusiveListenThread", "exclusiveListenThread", OptionParser::STORE_TRUE);
    getOptionParser().addOption("", "--ldLibraryPath", "ldLibraryPath", OptionParser::OPT_STRING, false);
    getOptionParser().addOption("", "--idleSleepTimeUs", "idleSleepTimeUs", IDLE_SLEEP_TIME_US);
    doInitOptions();
}

void WorkerBase::extractOptions() {
    getOptionParser().getOptionValue("port", _impl->port);
    getOptionParser().getOptionValue("httpPort", _impl->httpPort);
    getOptionParser().getOptionValue("logConf", _impl->logConfFile);
    getOptionParser().getOptionValue("workDir", _impl->cwdPath);
    getOptionParser().getOptionValue("deamon", _impl->deamon);
    getOptionParser().getOptionValue("threadNum", _impl->threadNum);
    getOptionParser().getOptionValue("ioThreadNum", _impl->ioThreadNum);
    getOptionParser().getOptionValue("queueSize", _impl->queueSize);
    getOptionParser().getOptionValue("recommendPort", _impl->recommendPort);
    getOptionParser().getOptionValue("anetTimeoutLoopIntervalMs", _impl->anetTimeoutLoopIntervalMs);
    getOptionParser().getOptionValue("exclusiveListenThread", _impl->exclusiveListenThread);
    getOptionParser().getOptionValue("ldLibraryPath", _impl->ldLibraryPath);
    getOptionParser().getOptionValue("idleSleepTimeUs", _impl->idleSleepTimeUs);
    doExtractOptions();
}

void WorkerBase::initDeamon() {
    if (!_impl->deamon) {
        return;
    }
    pid_t pid;
    if ((pid = fork()) != 0) {
        exit(0);
    }
    setsid();
    if ((pid = fork()) != 0) {
        exit(0);
    }
    umask(0);
    pid = getpid();
    string stdoutFile = "stdout." + StringUtil::toString(pid);
    string stderrFile = "stderr." + StringUtil::toString(pid);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    freopen(stdoutFile.c_str(), "w", stdout);
    freopen(stderrFile.c_str(), "w", stderr);
    freopen("/dev/null", "r", stdin);
#pragma GCC diagnostic pop
}

bool WorkerBase::changeCurrentPath(const string &path) {
    int ret = chdir(path.c_str());
    if (0 != ret) {
        AUTIL_LOG(ERROR,
                  "Failed to change current work directory"
                  "(ERROR CODE: %d)",
                  ret);
        return false;
    }
    return true;
}

bool WorkerBase::init(int argc, char **argv) {
    initOptions();

    if (!getOptionParser().parseArgs(argc, argv)) {
        fprintf(stderr, "WorkerBase option parse failed.\n");
        return false;
    }

    extractOptions();

    if (!_impl->ldLibraryPath.empty()) {
        static string LD_LIBRARY_PATH = "LD_LIBRARY_PATH";
        string rawLdLibraryPath = autil::EnvUtil::getEnv(LD_LIBRARY_PATH);
        string newLdLibraryPath = _impl->ldLibraryPath;
        if (!rawLdLibraryPath.empty()) {
            newLdLibraryPath = newLdLibraryPath + ":" + rawLdLibraryPath;
        }
        bool ret = autil::EnvUtil::setEnv(LD_LIBRARY_PATH, newLdLibraryPath, true);
        if (!ret) {
            fprintf(stderr, "set env LD_LIBRARY_PATH (%s) falied.\n", newLdLibraryPath.c_str());
        } else {
            fprintf(stderr, "set env LD_LIBRARY_PATH (%s) success.\n", newLdLibraryPath.c_str());
        }
    }

    if (!initMonitor()) {
        fprintf(stderr, "WorkerBase init monitor failed.\n");
        return false;
    }

    // get & change cwd
    if (_impl->cwdPath.empty()) {
        if (!PathUtil::getCurrentPath(_impl->cwdPath)) {
            fprintf(stderr, "Failed to get current path.\n");
            return false;
        }
    }

    FileSystem::mkDir(_impl->cwdPath, true);
    if (!changeCurrentPath(_impl->cwdPath)) {
        fprintf(stderr, "Failed to change current path to %s.\n", _impl->cwdPath.c_str());
        return false;
    }

    _impl->cwdPath = PathUtil::normalizePath(_impl->cwdPath);

    initDeamon();

    // init log
    if (!initLog()) {
        fprintf(stderr, "Init log failed.\n");
        return false;
    }

    if (!initRPCServer()) {
        AUTIL_LOG(ERROR, "WorkerBase init RPCServer failed.\n");
        return false;
    }

    if (!doInit()) {
        AUTIL_LOG(ERROR, "WorkerBase doInit failed.\n");
        return false;
    }

    if (!startRPCServer()) {
        AUTIL_LOG(ERROR, "WorkerBase start RPCServer failed.\n");
        return false;
    }

    AUTIL_LOG(INFO, "WorkerBase doInit success");
    return true;
}

bool WorkerBase::initRPCServer() {
    assert(!_impl->rpcServer);
    if (_impl->ioThreadNum < 1) {
        _impl->ioThreadNum = 1;
    }
    anet::ListenFdThreadModeEnum modeEnum =
        _impl->exclusiveListenThread ? anet::EXCLUSIVE_LISTEN_THREAD : anet::SHARE_THREAD;
    _impl->transport = new anet::Transport(_impl->ioThreadNum, modeEnum);
    if (_impl->anetTimeoutLoopIntervalMs > 0) {
        AUTIL_LOG(INFO, "set anet timeout loop interval [%d] ms", _impl->anetTimeoutLoopIntervalMs);
        _impl->transport->setTimeoutLoopInterval(_impl->anetTimeoutLoopIntervalMs);
    }
    _impl->rpcServer = new arpc::ANetRPCServer(_impl->transport, _impl->threadNum, _impl->queueSize);
    if (!_impl->rpcServer) {
        AUTIL_LOG(ERROR, "alloc ANetRPCServer failed.");
        return false;
    }
    return true;
}

bool WorkerBase::initHTTPRPCServer() { return initHTTPRPCServer(_impl->threadNum, _impl->queueSize); }

bool WorkerBase::initHTTPRPCServer(uint32_t threadNum, uint32_t queueSize) {
    _impl->httpRPCServer = new http_arpc::HTTPRPCServer(_impl->rpcServer, NULL, threadNum, queueSize);
    if (!_impl->httpRPCServer) {
        AUTIL_LOG(ERROR, "alloc HTTPRPCServer failed.");
        return false;
    }

    if (!_impl->httpRPCServer->StartThreads()) {
        AUTIL_LOG(ERROR, "start http rpc server failed");
        return false;
    }
    return true;
}

bool WorkerBase::registerService(RPCService *rpcService, arpc::ThreadPoolDescriptor tpd) {
    ScopedLock lock(_impl->mutex);
    if (_impl->rpcServer == NULL) {
        AUTIL_LOG(ERROR,
                  "rpc server is not inited, "
                  "can not register service now.");
        return false;
    }
    if (_impl->isRpcServerStarted) {
        AUTIL_LOG(ERROR,
                  "rpc server already start, "
                  "can not register service now.");
        return false;
    }

    if (!_impl->rpcServer->RegisterService(rpcService, tpd)) {
        AUTIL_LOG(ERROR, "register service failed");
        return false;
    }

    return true;
}

bool WorkerBase::startRPCServer() {
    assert(_impl->rpcServer);
    ScopedLock lock(_impl->mutex);
    if (!_impl->transport->start()) {
        AUTIL_LOG(ERROR, "start transport failed");
        return false;
    }
    std::function<bool(string)> listenFunc = std::bind(&arpc::ANetRPCServer::Listen,
                                                       _impl->rpcServer,
                                                       std::placeholders::_1,
                                                       5000,
                                                       arpc::MAX_IDLE_TIME,
                                                       arpc::LISTEN_BACKLOG);
    _impl->port = listen(listenFunc, _impl->port);
    if (_impl->port < 0) {
        _impl->isRpcServerStarted = false;
        return false;
    }
    if (_impl->httpRPCServer) {
        if (!_impl->httpRPCServer->StartPrivateTransport()) {
            AUTIL_LOG(ERROR, "HTTPRPCServer start failed");
            return false;
        }
        std::function<bool(string)> httpListenFunc = std::bind(&http_arpc::HTTPRPCServer::Listen,
                                                               _impl->httpRPCServer,
                                                               std::placeholders::_1,
                                                               http_arpc::TIMEOUT,
                                                               http_arpc::MAX_IDLE_TIME,
                                                               http_arpc::LISTEN_BACKLOG);
        _impl->httpPort = listen(httpListenFunc, _impl->httpPort);
        if (_impl->httpPort < 0) {
            _impl->isRpcServerStarted = false;
            return false;
        }
    }
    _impl->isRpcServerStarted = true;
    _impl->transport->setName("WBase");
    return true;
}

void WorkerBase::stopRPCServer() {
    closeRpcServer();
    destoryRpcServer();
}

void WorkerBase::closeRpcServer() {
    if (_impl->httpRPCServer) {
        _impl->httpRPCServer->Close();
        _impl->httpRPCServer->StopPrivateTransport();
        AUTIL_LOG(INFO, "httpRPCServer is stopped.");
    }
    if (_impl->rpcServer) {
        _impl->rpcServer->Close();
        if (_impl->transport) {
            _impl->transport->stop();
            _impl->transport->wait();
            DELETE_AND_SET_NULL(_impl->transport);
            AUTIL_LOG(INFO, "transport is stopped.");
        }
        _impl->rpcServer->stopThreadPools();
        AUTIL_LOG(INFO, "RpcServer is stopped.");
    }
}

void WorkerBase::destoryRpcServer() {
    if (_impl->httpRPCServer) {
        DELETE_AND_SET_NULL(_impl->httpRPCServer);
    }
    if (_impl->rpcServer) {
        DELETE_AND_SET_NULL(_impl->rpcServer);
    }
}

int32_t WorkerBase::listen(const std::function<bool(string)> &listenFunc, int32_t port) {
    string spec("tcp:0.0.0.0:");
    if (port != 0) {
        string address = spec + autil::StringUtil::toString(port);
        if (!listenFunc(address)) {
            AUTIL_LOG(ERROR, "Listen on %s failed", address.c_str());
            if (!_impl->recommendPort) {
                return -1;
            }
        } else {
            AUTIL_LOG(INFO, "Listen on %s success, recommend %d", address.c_str(), _impl->recommendPort);
            return port;
        }
    }

    srand((unsigned int)TimeUtility::currentTime());
    for (int32_t tryTime = 0; tryTime < 100; tryTime++) {
        int32_t tryPort = random() % (MAX_RANDOM_PORT - MIN_RANDOM_PORT) + MIN_RANDOM_PORT;
        string trySpec = spec + autil::StringUtil::toString(tryPort);
        AUTIL_LOG(INFO, "try Listen on %s", trySpec.c_str());
        if (listenFunc(trySpec)) {
            AUTIL_LOG(INFO, "Listen on %s success", trySpec.c_str());
            return tryPort;
        }
    }
    AUTIL_LOG(INFO, "try listen failed.");
    return -1;
}

bool WorkerBase::initLog() {

    if ("" == _impl->logConfFile) {
        fslib::ErrorCode errorCode = fslib::fs::FileSystem::isExist("worker_framework_alog.conf");
        if (fslib::EC_TRUE != errorCode) {
            AUTIL_ROOT_LOG_CONFIG();
            return true;
        }
        _impl->logConfFile = "worker_framework_alog.conf";
    }

    fslib::ErrorCode errorCode = fslib::fs::FileSystem::isExist(_impl->logConfFile);

    if (fslib::EC_TRUE == errorCode) {
        string logConfContent;
        if (FileSystem::readFile(_impl->logConfFile, logConfContent) != EC_OK) {
            std::cerr << "Failed to read Logger config file [" << _impl->logConfFile << std::endl;
            return false;
        }
        try {
            alog::Configurator::configureLoggerFromString(logConfContent.c_str());
        } catch (std::exception &e) {
            std::cerr << "Failed to configure logger. Logger config file [" << _impl->logConfFile << "], errorMsg ["
                      << e.what() << "]." << std::endl;
            return false;
        }
    } else {
        std::cerr << "log config file [" << _impl->logConfFile << "] doesn't exist. errorCode: [" << errorCode
                  << "], errorMsg: [" << fslib::fs::FileSystem::getErrorString(errorCode) << "]" << std::endl;
        return false;
    }

    return true;
}

bool WorkerBase::run() {
    isStop = false;
    registerSignalHandler();

    while (!isStop && !isStopped()) {
        _impl->reInit = false;
        if (!doStart()) {
            doStop();
            stopRPCServer();
            return false;
        }

        AUTIL_LOG(INFO, "worker is preparing to run...");
        while (!isStop && !isStopped()) {
            if (doPrepareToRun()) {
                break;
            }
            usleep(_impl->idleSleepTimeUs);
        }

        AUTIL_LOG(INFO, "worker is running...");
        while (!isStop && !_impl->reInit && !isStopped()) {
            usleep(_impl->idleSleepTimeUs);
            doIdle();
        }
        doStop();
        AUTIL_LOG(INFO, "worker is stopped.");
    }
    stopRPCServer();
    return true;
}

}; // namespace worker_framework
