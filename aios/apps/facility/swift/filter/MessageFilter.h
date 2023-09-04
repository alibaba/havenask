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
#include "swift/common/FileMessageMeta.h"
#include "swift/common/MemoryMessage.h"
#include "swift/common/MessageMeta.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"

namespace swift {
namespace filter {

class MessageFilter {
public:
    MessageFilter(const protocol::Filter &filter) {
        from = filter.from();
        to = filter.to();
        mergedTo = filter.mergedto();
        mask = filter.uint8filtermask();
        result = filter.uint8maskresult();
    }
    ~MessageFilter() {}

private:
    MessageFilter(const MessageFilter &);
    MessageFilter &operator=(const MessageFilter &);

public:
    bool filter(const protocol::Message &msg) const;
    bool filter(const protocol::flat::Message &msg) const;
    bool filter(const common::MemoryMessage &memMsg) const;
    bool filter(const common::FileMessageMeta &meta) const;
    bool filter(const common::MergedMessageMeta &meta) const;

private:
    bool rangeFilterForMerge(uint16_t payload) const;
    bool rangeFilter(uint16_t payload) const;
    bool maskFilter(uint8_t maskPayload) const;

private:
    uint16_t from;
    uint16_t to;
    uint16_t mergedTo;
    uint8_t mask;
    uint8_t result;

private:
    friend class MessageFilterTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MessageFilter);
/////////////////////////////////////////////////////////////////

inline bool MessageFilter::filter(const protocol::Message &msg) const {
    if (msg.merged()) {
        return rangeFilterForMerge(msg.uint16payload()) && maskFilter(msg.uint8maskpayload());
    } else {
        return rangeFilter(msg.uint16payload()) && maskFilter(msg.uint8maskpayload());
    }
}

inline bool MessageFilter::filter(const protocol::flat::Message &msg) const {
    if (msg.merged()) {
        return rangeFilterForMerge(msg.uint16Payload()) && maskFilter(msg.uint8MaskPayload());
    } else {
        return rangeFilter(msg.uint16Payload()) && maskFilter(msg.uint8MaskPayload());
    }
}

inline bool MessageFilter::filter(const common::MemoryMessage &memMsg) const {
    if (memMsg.isMerged()) {
        return rangeFilterForMerge(memMsg.getPayload()) && maskFilter(memMsg.getMaskPayload());
    } else {
        return rangeFilter(memMsg.getPayload()) && maskFilter(memMsg.getMaskPayload());
    }
}

inline bool MessageFilter::filter(const common::FileMessageMeta &meta) const {
    if (meta.isMerged) {
        return rangeFilterForMerge(meta.payload) && maskFilter(meta.maskPayload);
    } else {
        return rangeFilter(meta.payload) && maskFilter(meta.maskPayload);
    }
}

inline bool MessageFilter::filter(const common::MergedMessageMeta &meta) const {
    return rangeFilter(meta.payload) && maskFilter(meta.maskPayload);
}

inline bool MessageFilter::rangeFilter(uint16_t payload) const { return payload >= from && payload <= to; }
inline bool MessageFilter::rangeFilterForMerge(uint16_t payload) const {
    return payload >= from && payload <= mergedTo;
}

inline bool MessageFilter::maskFilter(uint8_t maskPayload) const { return (maskPayload & mask) == result; }

} // namespace filter
} // namespace swift
