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

#include "autil/Log.h"
#include "autil/OptionParser.h"
#include "aios/network/gig/multi_call/rpc/GigRpcServer.h"
#include "suez/worker/SuezServerWorkerHandlerFactory.h"

namespace isearch {
namespace application {

class Server
{
public:
    Server();
     ~Server();

public:
    bool init(const autil::OptionParser& optionParser);
    bool run();

private:
    bool initARPCServer();
    void prepareArpcDesc(multi_call::ArpcServerDescription& desc);
    bool initSuezWorkerHandlerFactory(const autil::OptionParser& optionParser);
    void stopRPCServer();
    void stopSuezWorkerHandelFactory();
    void release();
private:
    std::shared_ptr<multi_call::GigRpcServer> _rpcServer;
    std::shared_ptr<suez::SuezServerWorkerHandlerFactory> _suezWorkerHandlerFactory;
    AUTIL_LOG_DECLARE();
};

}
}
