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

#include <stddef.h>
#include <stdint.h>

namespace swift {
namespace protocol {
class ConsumptionRequest;
class MessageResponse;
} // namespace protocol

namespace replication {

class MessageProducer {
public:
    MessageProducer(uint64_t selfVersion, bool readCommittedMsg);
    virtual ~MessageProducer();

public:
    int64_t getSelfVersion() const { return _selfVersion; }
    void produce(int64_t startId, size_t count, protocol::MessageResponse *result);

private:
    void prepareRequest(protocol::ConsumptionRequest *request, int64_t startId, size_t msgCount);

    // virtual for test
    virtual void produce(protocol::ConsumptionRequest *request, protocol::MessageResponse *response) = 0;

private:
    const uint64_t _selfVersion;
    const bool _readCommittedMsg;
};

} // namespace replication
} // namespace swift
