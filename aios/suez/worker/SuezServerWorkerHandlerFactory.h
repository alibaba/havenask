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
#pragma once

#include "aios/network/http_arpc/HTTPRPCServer.h"
#include "autil/LoopThread.h"
#include "suez/sdk/RpcServer.h"
#include "suez/worker/EnvParam.h"
#include "suez/worker/TaskExecutor.h"

namespace multi_call {
class GigRpcServer;
}

namespace suez {
class HeartbeatManager;
class SelfKillerService;
class DebugServiceImpl;
class KMonitorManager;
} // namespace suez

namespace suez {

class SuezServerWorkerHandlerFactory {
public:
    SuezServerWorkerHandlerFactory(const std::shared_ptr<multi_call::GigRpcServer> &rpcServer,
                                   const std::string &installRoot);
    ~SuezServerWorkerHandlerFactory();

private:
    SuezServerWorkerHandlerFactory(const SuezServerWorkerHandlerFactory &);
    SuezServerWorkerHandlerFactory &operator=(const SuezServerWorkerHandlerFactory &);

public:
    bool initilize(const EnvParam &param);
    void release();
    const std::shared_ptr<TaskExecutor> &getTaskExecutor() const { return _taskExecutor; }

private:
    bool startHttp(const EnvParam &param);
    bool startRdma(const EnvParam &param);
    bool startGigRpcServer(const EnvParam &param);
    void waitForDebugger(const EnvParam &param);
    bool initTaskExecutor(const EnvParam &param);
    void report();
    bool initGrpcServer(const EnvParam &param);
    bool getGigGrpcDesc(const EnvParam &param, const std::string &role, multi_call::GrpcServerDescription &desc) const;

private:
    std::shared_ptr<multi_call::GigRpcServer> _rpcServer;
    std::unique_ptr<KMonitorManager> _kmonManager;
    std::string _installRoot;
    RpcServer _rpcServerWrapper;
    std::shared_ptr<TaskExecutor> _taskExecutor;
    std::shared_ptr<DebugServiceImpl> _debugService;
    std::shared_ptr<HeartbeatManager> _hbManager;
    std::shared_ptr<SelfKillerService> _selfKiller;
    autil::LoopThreadPtr _reportThread;
};

} // namespace suez
