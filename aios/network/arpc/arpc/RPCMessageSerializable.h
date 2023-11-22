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
#ifndef ARPC_RPCMESSAGESERIALIZABLE_H
#define ARPC_RPCMESSAGESERIALIZABLE_H
#include <memory>
#include <stdint.h>

#include "aios/network/anet/databufferserializable.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "google/protobuf/arena.h"

ARPC_BEGIN_NAMESPACE(arpc);

class RPCMessageSerializable : public anet::DataBufferSerializable {
public:
    RPCMessageSerializable(RPCMessage *header, RPCMessage *body, const std::shared_ptr<google::protobuf::Arena> &arena);
    ~RPCMessageSerializable();

public:
    static bool serializeMessage(const RPCMessage* message, anet::DataBuffer *outputBuffer);
    static bool serializeMessage(const RPCMessage *message, size_t messageSize, anet::DataBuffer *outputBuffer);
    static bool deserializeMessage(anet::DataBuffer *inputBuffer, RPCMessage *message, int32_t &leftPacketDataLen);
    
    RPCMessage *getHeader() { return _header; }
    RPCMessage *getBody() { return _body; }

    // override anet::DataBufferSerializable
    bool serialize(anet::DataBuffer *outputBuffer) const override;
    bool deserialize(anet::DataBuffer *inputBuffer, int length = 0) override;
    int64_t getSpaceUsed() override { return _header->SpaceUsed() + _body->SpaceUsed(); }
    size_t getSerializedSize() const override { return 4 + _headerSerializedSize + 4 + _bodySerializedSize; }

private:
    std::shared_ptr<google::protobuf::Arena> _arena;
    RPCMessage *_header;
    RPCMessage *_body;
    size_t _headerSerializedSize{0};
    size_t _bodySerializedSize{0};
};

ARPC_END_NAMESPACE(arpc);

#endif // ARPC_RPCMESSAGESERIALIZABLE_H
