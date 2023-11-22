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

#include "indexlib/document/IMessageIterator.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace build_service { namespace document {

class SwiftMessageIterator : public indexlibv2::document::IMessageIterator
{
public:
    SwiftMessageIterator(const swift::protocol::Messages* msgs) : _msgs(msgs), _cursor(0) {}

    ~SwiftMessageIterator() {}

public:
    bool HasNext() const override { return (_msgs != nullptr) && _cursor < _msgs->msgs_size(); }
    std::pair<autil::StringView, int64_t> Next() override
    {
        const auto& msg = _msgs->msgs(_cursor++);
        const std::string& strMsg = msg.data();
        return std::make_pair(autil::StringView(strMsg.data(), strMsg.size()), msg.timestamp());
    }
    size_t Size() const override { return (_msgs == nullptr) ? 0 : _msgs->msgs_size(); }
    size_t EstimateMemoryUse() const override
    {
        if (_msgs == nullptr) {
            return 0;
        }
        size_t memUse = 0;
        for (size_t i = 0; i < _msgs->msgs_size(); i++) {
            const auto& msg = _msgs->msgs(i);
            memUse += (msg.data().size() + sizeof(msg.timestamp()));
        }
        return memUse;
    };

private:
    const swift::protocol::Messages* _msgs;
    size_t _cursor;
};

}} // namespace build_service::document
