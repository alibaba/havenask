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
#include "swift/broker/service/RequestChecker.h"

#include <iosfwd>
#include <limits>
#include <string>

#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace swift::protocol;
namespace swift {
namespace service {
AUTIL_LOG_SETUP(swift, RequestChecker);
static int64_t MIN_GET_MESSAGE_SIZE = 1024;
static int64_t MAX_GET_MESSAGE_SIZE = 64 * 1024 * 1024;

RequestChecker::RequestChecker() : _maxGetMessageSize(MAX_GET_MESSAGE_SIZE) {}

RequestChecker::~RequestChecker() {}

bool RequestChecker::getMessage(const ConsumptionRequest *request) {
    if (!request->has_topicname() || !request->has_partitionid() || !request->has_startid() ||
        !(isValidPayload(request->filter().from())) || !(isValidPayload(request->filter().to())) ||
        !(isValidMaskPayload(request->filter().uint8filtermask())) ||
        !(isValidMaskPayload(request->filter().uint8maskresult()))) {
        AUTIL_LOG(ERROR, "Invalid broker request[%s].", request->ShortDebugString().c_str());
        return false;
    }
    if (!request->has_count()) {
        (const_cast<ConsumptionRequest *>(request))->set_count(1);
        return true;
    }
    int64_t size = request->maxtotalsize();
    if (size > _maxGetMessageSize) {
        AUTIL_LOG(DEBUG, "not allow max total size[%ld] , change it to %ld(maxCount)", size, _maxGetMessageSize);
        (const_cast<ConsumptionRequest *>(request))->set_maxtotalsize(_maxGetMessageSize);
    }
    return true;
}

bool RequestChecker::sendMessage(const ProductionRequest *request) {
    if (!request->has_topicname() || !request->has_partitionid()) {
        AUTIL_LOG(ERROR, "Invalid broker request[%s].", request->ShortDebugString().c_str());
        return false;
    }

    for (int i = 0; i < request->msgs_size(); ++i) {
        if (!(isValidPayload(request->msgs(i).uint16payload())) ||
            !(isValidMaskPayload(request->msgs(i).uint8maskpayload()))) {
            AUTIL_LOG(ERROR, "Invalid broker message[%s].", request->msgs(i).ShortDebugString().c_str());
            return false;
        }
    }

    return true;
}

bool RequestChecker::getMaxMessageId(const MessageIdRequest *request) {
    if (!request->has_topicname() || !request->has_partitionid()) {
        AUTIL_LOG(ERROR, "Invalid broker request[%s].", request->ShortDebugString().c_str());
        return false;
    }
    return true;
}

bool RequestChecker::getMinMessageIdByTime(const MessageIdRequest *request) {
    if (!request->has_topicname() || !request->has_partitionid() || !request->has_timestamp()) {
        AUTIL_LOG(ERROR, "Invalid broker request[%s].", request->ShortDebugString().c_str());
        return false;
    }
    return true;
}

bool RequestChecker::isValidPayload(uint32_t payload) { return payload <= numeric_limits<uint16_t>::max(); }

bool RequestChecker::isValidMaskPayload(uint32_t maskPayload) { return maskPayload <= numeric_limits<uint8_t>::max(); }
void RequestChecker::setMaxGetMessageSize(int64_t size) {
    if (size >= MIN_GET_MESSAGE_SIZE && size <= MAX_GET_MESSAGE_SIZE) {
        _maxGetMessageSize = size;
    } else {
        AUTIL_LOG(WARN,
                  "max get message size must be in [%ld, %ld], now is %ld",
                  MIN_GET_MESSAGE_SIZE,
                  MAX_GET_MESSAGE_SIZE,
                  size);
    }
}

} // namespace service
} // namespace swift
