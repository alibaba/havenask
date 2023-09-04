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

#include "suez/sdk/KMonitorMetaInfo.h"

namespace future_lite {
class Executor;
}

namespace suez {

class RpcServer;

struct SearchInitParam {
    RpcServer *rpcServer = nullptr;
    KMonitorMetaInfo kmonMetaInfo;
    std::string installRoot; // suez_navi
    future_lite::Executor *asyncInterExecutor = nullptr;
    future_lite::Executor *asyncIntraExecutor = nullptr;
};

} // namespace suez
