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
#ifndef ISEARCH_MULTI_CALL_GIGSTREAMMESSAGE_H
#define ISEARCH_MULTI_CALL_GIGSTREAMMESSAGE_H

#include <google/protobuf/message.h>

#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

class GigStreamMessage
{
public:
    GigStreamMessage()
        : partId(INVALID_PART_ID)
        , message(nullptr)
        , eof(false)
        , handlerId(-1)
        , netLatencyUs(0) {
    }

public:
    void initArena() {
        arena.reset(new google::protobuf::Arena());
    }

public:
    PartIdTy partId;
    google::protobuf::Message *message;
    bool eof;
    std::shared_ptr<google::protobuf::Arena> arena;
    int64_t handlerId;
    int64_t netLatencyUs;
};

typedef std::map<PartIdTy, GigStreamMessage> GigStreamMessageMap;
} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGSTREAMMESSAGE_H
