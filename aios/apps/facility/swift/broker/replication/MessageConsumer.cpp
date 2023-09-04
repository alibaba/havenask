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
#include "swift/broker/replication/MessageConsumer.h"

#include "autil/result/Errors.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"

namespace swift {
namespace replication {

using autil::result::RuntimeError;

MessageConsumer::MessageConsumer() {}

MessageConsumer::~MessageConsumer() {}

autil::Result<int64_t> MessageConsumer::consume(protocol::MessageResponse *response) {
    if (!checkInput(response)) {
        return RuntimeError::make("input invalid");
    }
    return doConsume(response);
}

bool MessageConsumer::checkInput(protocol::MessageResponse *response) const {
    if (!response->has_messageformat()) {
        return false;
    }
    if (response->messageformat() == protocol::MF_FB) {
        return response->has_fbmsgs();
    }
    return true;
}

} // namespace replication
} // namespace swift
