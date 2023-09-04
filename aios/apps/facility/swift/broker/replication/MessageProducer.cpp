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
#include "swift/broker/replication/MessageProducer.h"

#include "autil/TimeUtility.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"

namespace swift {
namespace replication {

MessageProducer::MessageProducer(uint64_t selfVersion, bool readCommittedMsg)
    : _selfVersion(selfVersion), _readCommittedMsg(readCommittedMsg) {}

MessageProducer::~MessageProducer() {}

void MessageProducer::produce(int64_t startId, size_t count, protocol::MessageResponse *response) {
    protocol::ConsumptionRequest request;
    prepareRequest(&request, startId, count);
    produce(&request, response);
}

void MessageProducer::prepareRequest(protocol::ConsumptionRequest *request, int64_t startId, size_t msgCount) {
    request->set_selfversion(_selfVersion);
    request->set_startid(startId);
    request->set_count(msgCount);
    request->set_generatedtime(autil::TimeUtility::currentTime());
    request->set_readcommittedmsg(_readCommittedMsg);
}

} // namespace replication
} // namespace swift
