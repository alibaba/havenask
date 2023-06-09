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
#include "aios/network/gig/multi_call/java/arpc/GigRPCMessageCodec.h"
#include "aios/network/arpc/arpc/proto/msg_header.pb.h"
#include "aios/network/gig/multi_call/java/arpc/GigRPCMessageSerializable.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigRPCMessageCodec);

GigRPCMessageCodec::GigRPCMessageCodec(anet::IPacketFactory *packetFactory)
    : arpc::ANetRPCMessageCodec(packetFactory) {}

GigRPCMessageCodec::~GigRPCMessageCodec() {}

anet::Packet *GigRPCMessageCodec::EncodeRequest(const GigCodecContext *context,
                                                uint32_t pcode,
                                                version_t version) {
    anet::DataBufferSerializable *serializable = NULL;

    if (version == ARPC_VERSION_1) {
        if (pcode & ADVANCE_PACKET_MASK) {
            AUTIL_LOG(ERROR, "Invalid callId [%ud]", pcode);
            return NULL;
        }

        pcode |= ADVANCE_PACKET_MASK;

        arpc::RpcMsgHeader *msgHeader =
            google::protobuf::Arena::CreateMessage<arpc::RpcMsgHeader>(
                context->arena.get());
        msgHeader->set_enabletrace(context->enableTrace);
        msgHeader->set_userpayload(context->userPayload);

        serializable = new (nothrow) GigRPCMessageSerializable(
            msgHeader, (std::string *)context->request);
    } else {
        AUTIL_LOG(ERROR, "Invalid version [%d]", version);
        return NULL;
    }

    anet::Packet *packet = createPacket(pcode, serializable);

    if (packet == NULL) {
        delete serializable;
        return NULL;
    }

    packet->setPacketVersion(version);
    return packet;
}

} // namespace multi_call
