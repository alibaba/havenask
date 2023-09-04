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
#include "Server.h"
#include <unistd.h>
#include "aios/network/arpc/arpc/ANetRPCServer.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace application {

const static string RPC_PORT = "port";
const static string RPC_THREAD_NUM = "rpc_thread_num";
const static string RPC_QUEUE_SIZE = "rpc_queue_size";
const static string RPC_IO_THREAD_NUM = "rpc_iothread_number";

volatile static bool isStop = false;
static sighandler_t __func_int;
static sighandler_t __func_term;
static sighandler_t __func_user1;
static sighandler_t __func_user2;

static void handleSignal(int sig) {
    if (!isStop) {
        fprintf(stdout, "receive signal [%d], stop", sig);
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
    signal(SIGINT, __func_int);
    signal(SIGTERM, __func_term);
    signal(SIGUSR1, __func_user1);
    signal(SIGUSR2, __func_user2);
}

AUTIL_LOG_SETUP(ha3, Server);
Server::Server() {
}

Server::~Server() {
    release();
}

void Server::release() {
    AUTIL_LOG(INFO, "start to Server::release() ...");
    stopRPCServer();
    stopSuezWorkerHandelFactory();
    resetSignalHandler();
    AUTIL_LOG(INFO, "server(%p) has been stopped.", this);
}

bool Server::init(const OptionParser& optionParser) {
    AUTIL_LOG(INFO, "server init");
    if (!initARPCServer()) {
        AUTIL_LOG(ERROR, "init arpc server failed.");
        return false;
    }
    AUTIL_LOG(INFO, "init arpc server with port %d", _rpcServer->getArpcServer()->GetListenPort())
    AUTIL_LOG(INFO, "init arpc server success.")
    if (!initSuezWorkerHandlerFactory(optionParser)) {
        AUTIL_LOG(ERROR, "init suez worker handler factory failed.");
        return false;
    }
    AUTIL_LOG(INFO, "init suez worker handler factory sucess.");
    AUTIL_LOG(INFO, "initWatchThread sucess");
    return true;
}

bool Server::initARPCServer() {
    assert(!_rpcServer);
    _rpcServer.reset(new multi_call::GigRpcServer);

    AUTIL_LOG(INFO, "start to Server::initARPCServer() ...");
    multi_call::ArpcServerDescription desc;
    prepareArpcDesc(desc);
    return _rpcServer->startArpcServer(desc);
}

void Server::prepareArpcDesc(multi_call::ArpcServerDescription& desc) {
    desc.port = autil::EnvUtil::getEnv(RPC_PORT);
    AUTIL_LOG(INFO, "prepare arpc with port %s", desc.port.c_str());
    desc.name = "Ha3Arpc";
    desc.threadNum = autil::EnvUtil::getEnv(RPC_THREAD_NUM, 30);
    desc.queueSize = autil::EnvUtil::getEnv(RPC_QUEUE_SIZE, 1000);
    desc.ioThreadNum = autil::EnvUtil::getEnv(RPC_IO_THREAD_NUM, 5);
}

bool Server::initSuezWorkerHandlerFactory(const OptionParser& optionParser) {
    string rootDir;
    if (!optionParser.getOptionValue("rootDir", rootDir)) {
        AUTIL_LOG(ERROR, "root dir must be specified.");
        return false;
    }
    suez::EnvParam param;
    if (!param.init()) {
        AUTIL_LOG(ERROR, "init env param failed.");
        return false;
    }
    _suezWorkerHandlerFactory.reset(new suez::SuezServerWorkerHandlerFactory(_rpcServer, rootDir));
    if (!_suezWorkerHandlerFactory->initilize(param)) {
        _suezWorkerHandlerFactory.reset();
        return false;
    }
    return true;
}

void Server::stopRPCServer() {
    if (_rpcServer) {
        _rpcServer->release();
        _rpcServer.reset();
    }
}

void Server::stopSuezWorkerHandelFactory() {
    if (_suezWorkerHandlerFactory) {
        _suezWorkerHandlerFactory->release();
        _suezWorkerHandlerFactory.reset();
    }
}

bool Server::run() {
    isStop = false;
    registerSignalHandler();
    while(!isStop) {
        usleep(200);
    }
    stopRPCServer();
    stopSuezWorkerHandelFactory();
    return true;
}

}
}
