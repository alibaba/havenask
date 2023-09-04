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
#ifndef ARPC_MESSAGESERIALIZABLE_H
#define ARPC_MESSAGESERIALIZABLE_H

#include <memory>
#include <stdint.h>

#include "aios/network/anet/databufferserializable.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "google/protobuf/arena.h"

ARPC_BEGIN_NAMESPACE(arpc);

class MessageSerializable : public anet::DataBufferSerializable {
public:
    MessageSerializable(RPCMessage *message, const std::shared_ptr<google::protobuf::Arena> &arena);
    virtual ~MessageSerializable();

public:
    virtual bool serialize(anet::DataBuffer *outputBuffer) const;
    virtual bool deserialize(anet::DataBuffer *inputBuffer, int length = 0);
    RPCMessage *getMessage() const { return _message; }
    virtual int64_t getSpaceUsed() { return _message->SpaceUsed(); }

private:
    std::shared_ptr<google::protobuf::Arena> _arena;
    RPCMessage *_message;
};

ARPC_END_NAMESPACE(arpc);

#endif // ARPC_MESSAGESERIALIZABLE_H
