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

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace protocol {
class ConsumptionRequest;
class MessageIdRequest;
class ProductionRequest;
} // namespace protocol
} // namespace swift

namespace swift {
namespace service {

class RequestChecker {
public:
    RequestChecker();
    ~RequestChecker();

private:
    RequestChecker(const RequestChecker &);
    RequestChecker &operator=(const RequestChecker &);

public:
    bool getMessage(const ::swift::protocol::ConsumptionRequest *request);
    bool sendMessage(const ::swift::protocol::ProductionRequest *request);
    bool getMaxMessageId(const ::swift::protocol::MessageIdRequest *request);
    bool getMinMessageIdByTime(const ::swift::protocol::MessageIdRequest *request);

public:
    void setMaxGetMessageSize(int64_t size);

private:
    bool isValidPayload(uint32_t payload);
    bool isValidMaskPayload(uint32_t maskPayload);

private:
    int64_t _maxGetMessageSize;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(RequestChecker);

} // namespace service
} // namespace swift
