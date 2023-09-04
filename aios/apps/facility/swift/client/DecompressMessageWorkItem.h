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

#include "autil/Log.h"
#include "autil/WorkItem.h"
#include "swift/client/Notifier.h"
#include "swift/common/Common.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/MessageCompressor.h"

namespace swift {
namespace client {

class DecompressMessageWorkItem : public autil::WorkItem {
public:
    DecompressMessageWorkItem(protocol::MessageResponse *response, Notifier *notifier)
        : _response(response), _notifier(notifier) {}
    ~DecompressMessageWorkItem() {}

private:
    DecompressMessageWorkItem(const DecompressMessageWorkItem &);
    DecompressMessageWorkItem &operator=(const DecompressMessageWorkItem &);

public:
    virtual void process() {
        if (_response) {
            if (protocol::ERROR_NONE != protocol::MessageCompressor::decompressResponseMessage(_response)) {
                _response->mutable_errorinfo()->set_errcode(protocol::ERROR_DECOMPRESS_MESSAGE);
                AUTIL_LOG(ERROR, "decompress message error.");
            }
        }
        if (_notifier) {
            _notifier->notify();
        }
    }

private:
    protocol::MessageResponse *_response;
    Notifier *_notifier;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(DecompressMessageWorkItem);

} // namespace client
} // namespace swift
