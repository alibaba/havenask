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
#include "multi_call/rpc/GigRpcController.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/http_arpc/HTTPRPCControllerBase.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigRpcController);

std::string GigRpcController::getClientIp() const {
    auto protocolController = getProtocolController();
    auto anetRpcController = dynamic_cast<arpc::ANetRPCController *>(protocolController);
    if (anetRpcController) {
        return "arpc " + anetRpcController->GetClientAddress();
    } else {
        auto httpController = dynamic_cast<http_arpc::HTTPRPCControllerBase *>(protocolController);
        if (httpController) {
            return "http " + httpController->GetAddr();
        }
    }
    return "unknown";
}

}
