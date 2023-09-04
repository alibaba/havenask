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
#include <stdint.h>

#include "swift/broker/replication/MessageProducer.h"
#include "swift/client/SwiftTransportAdapter.h"
#include "swift/client/TransportClosure.h"

namespace swift {
namespace protocol {
class ConsumptionRequest;
class MessageResponse;
} // namespace protocol

namespace replication {

class MessageProducerImpl final : public MessageProducer {
    using GetMessageClient = client::SwiftTransportAdapter<client::TRT_GETMESSAGE>;

public:
    MessageProducerImpl(uint64_t selfVersion, bool readCommittedMsg, std::unique_ptr<GetMessageClient> client);
    ~MessageProducerImpl();

private:
    void produce(protocol::ConsumptionRequest *request, protocol::MessageResponse *response) override;

private:
    std::unique_ptr<GetMessageClient> _client;
};

} // namespace replication
} // namespace swift
