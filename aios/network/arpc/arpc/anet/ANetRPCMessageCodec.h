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
#ifndef ARPC_ANETRPCMESSAGECODEC_H
#define ARPC_ANETRPCMESSAGECODEC_H
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/arpc/arpc/util/Log.h"

ARPC_BEGIN_NAMESPACE(arpc);

class ANetRPCMessageCodec : public MessageCodec {
public:
    ANetRPCMessageCodec(anet::IPacketFactory *packetFactory);
    ~ANetRPCMessageCodec();

public:
    anet::Packet *EncodeRequest(const CodecContext *context, version_t version) const;
    CodecContext *DecodeRequest(anet::Packet *packet, const RPCServer *rpcServer, version_t version) const;
    anet::Packet *EncodeResponse(RPCMessage *response,
                                 Tracer *tracer,
                                 version_t version,
                                 const std::shared_ptr<google::protobuf::Arena> &arena) const;

protected:
    anet::Packet *createPacket(uint32_t pcode, anet::DataBufferSerializable *serializable) const;

private:
    friend class ANetRPCMessageCodecTest;

protected:
    anet::IPacketFactory *_packetFactory;
};

TYPEDEF_PTR(ANetRPCMessageCodec);
ARPC_END_NAMESPACE(arpc);

#endif // ARPC_ANETRPCMESSAGECODEC_H
