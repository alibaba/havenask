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

#include <stdint.h>

#include "autil/result/Result.h"
#include "swift/broker/replication/MessageConsumer.h"

namespace swift {
namespace protocol {
class MessageResponse;
class ProductionRequest;
} // namespace protocol
} // namespace swift

namespace swift {
namespace service {

class BrokerPartition;

class LocalMessageConsumer final : public replication::MessageConsumer {
public:
    explicit LocalMessageConsumer(BrokerPartition *brokerPartition);
    ~LocalMessageConsumer();

private:
    autil::Result<int64_t> doConsume(protocol::MessageResponse *response) override;

    void convertToProductionRequest(protocol::ProductionRequest *request, protocol::MessageResponse *response);

private:
    BrokerPartition *_brokerPartition;
};

} // namespace service
} // namespace swift
