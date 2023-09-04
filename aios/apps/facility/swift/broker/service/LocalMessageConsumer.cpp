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
#include "swift/broker/service/LocalMessageConsumer.h"

#include <cstdint>
#include <string>

#include "autil/result/Errors.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {
namespace service {

using autil::result::RuntimeError;

LocalMessageConsumer::LocalMessageConsumer(BrokerPartition *brokerPartition) : _brokerPartition(brokerPartition) {}

LocalMessageConsumer::~LocalMessageConsumer() { _brokerPartition = nullptr; }

autil::Result<int64_t> LocalMessageConsumer::doConsume(protocol::MessageResponse *response) {
    protocol::ProductionRequest request;
    convertToProductionRequest(&request, response);
    response->Clear();
    _brokerPartition->addReplicationMessage(&request, response);
    if (response->has_committedid()) {
        return {response->committedid()};
    } else {
        return RuntimeError::make("%s", response->errorinfo().ShortDebugString().c_str());
    }
}

void LocalMessageConsumer::convertToProductionRequest(protocol::ProductionRequest *request,
                                                      protocol::MessageResponse *response) {
    request->set_messageformat(response->messageformat());
    if (response->has_compressedmsgs()) {
        request->mutable_compressedmsgs()->swap(*response->mutable_compressedmsgs());
    } else {
        if (response->messageformat() == protocol::MF_PB) {
            request->mutable_msgs()->Swap(response->mutable_msgs());
        } else {
            request->mutable_fbmsgs()->swap(*response->mutable_fbmsgs());
        }
    }
}

} // namespace service
} // namespace swift
