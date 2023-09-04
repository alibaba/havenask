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
#ifndef ISEARCH_MULTI_CALL_GIGRPCMESSAGESERIALIZABLE_H
#define ISEARCH_MULTI_CALL_GIGRPCMESSAGESERIALIZABLE_H

#include <string>

#include "aios/network/anet/databufferserializable.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "google/protobuf/arena.h"

namespace multi_call {

class GigRPCMessageSerializable : public anet::DataBufferSerializable
{
public:
    GigRPCMessageSerializable(RPCMessage *header, std::string *body);
    ~GigRPCMessageSerializable();

private:
    GigRPCMessageSerializable(const GigRPCMessageSerializable &);
    GigRPCMessageSerializable &operator=(const GigRPCMessageSerializable &);

public:
    virtual bool serialize(anet::DataBuffer *outputBuffer) const;
    virtual bool deserialize(anet::DataBuffer *inputBuffer, int length = 0);

private:
    bool serializeStringMessage(const std::string *message, anet::DataBuffer *outputBuffer) const;
    bool deserializeStringMessage(anet::DataBuffer *inputBuffer, std::string *message,
                                  int32_t &leftPacketDataLen);

private:
    RPCMessage *_header;
    std::string *_body;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigRPCMessageSerializable);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGRPCMESSAGESERIALIZABLE_H
