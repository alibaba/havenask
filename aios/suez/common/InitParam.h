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

#include <memory>
#include <string>

#include "suez/sdk/KMonitorMetaInfo.h"

namespace autil {
class ThreadPool;
}

namespace future_lite {
class Executor;
}

namespace suez {

class DeployManager;
;
class RpcServer;

struct InitParam {
public:
    InitParam() {}

public:
    RpcServer *rpcServer = nullptr;
    KMonitorMetaInfo kmonMetaInfo;
    future_lite::Executor *asyncInterExecutor = nullptr;
    future_lite::Executor *asyncIntraExecutor = nullptr;

    autil::ThreadPool *deployThreadPool = nullptr;
    autil::ThreadPool *loadThreadPool = nullptr;

    DeployManager *deployManager = nullptr;
    std::string installRoot;
};

} // namespace suez
