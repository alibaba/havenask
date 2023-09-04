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
#include "aios/network/anet/directpacketstreamer.h"

#include <assert.h>
#include <string.h>

#include "aios/network/anet/channel.h"
#include "aios/network/anet/channelpool.h"
#include "aios/network/anet/controlpacket.h"
#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/debug.h"
#include "aios/network/anet/defaultpacketstreamer.h"
#include "aios/network/anet/directpacket.h"
#include "aios/network/anet/directplaceholder.h"
#include "aios/network/anet/directstreamingcontext.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/streamingcontext.h"

namespace anet {

static const DirectPlaceholder kEmptyPlaceholder{};

Packet *DirectPacketFactory::createPacket(int pcode) { return createDirectPacket(pcode); }

DirectPacket *DirectPacketFactory::createDirectPacket(int pcode) { return new DirectPacket(); }

DirectPacketStreamer::DirectPacketStreamer() : DefaultPacketStreamer(&_directPacketFactory) {}

DirectPacketStreamer::~DirectPacketStreamer() {}

StreamingContext *DirectPacketStreamer::createContext() { return new DirectStreamingContext; }

bool DirectPacketStreamer::encode(Packet *packet, DataBuffer *output, DirectPayload &payloadToWrite) {
    auto directPacket = dynamic_cast<DirectPacket *>(packet);
    if (nullptr == directPacket) {
        ANET_LOG(ERROR, "only DirectPacket is supoorted for DirectPacketStreamer");
        return false;
    }

    PacketHeader *header = directPacket->getPacketHeader();
    // 为了当encode失败恢复时用
    int oldLen = output->getDataLen();
    // dataLen的位置
    int dataLenOffset = -1;

    int msgSize = 0;
    int msgSizeOffset = -1;
    const auto &payload = directPacket->getPayload();
    int payloadSize = payload.getLen();
    int toReceiveSize = directPacket->hasPlaceholder() ? directPacket->getPlaceholder().getLen() : 0;

    // pcode offset
    int pcodeOffset = -1;
    if (_existPacketHeader) {
        output->writeInt32(ANET_PACKET_FLAG);
        output->writeInt32(header->_chid);

        pcodeOffset = output->getDataLen();
        output->writeInt32(header->_pcode);

        dataLenOffset = output->getDataLen();
        output->writeInt32(0);

        // decode direct packet protocol header
        output->writeInt32(ANET_DIRECT_PACKET_FLAG);
        output->writeInt32(ANET_DIRECT_PACKET_VER);
        output->writeInt64(0UL);
        msgSizeOffset = output->getDataLen();
        output->writeInt32(0UL);
        output->writeInt32(payloadSize);
        output->writeInt32(toReceiveSize);
    }
    // 写数据
    if (directPacket->encode(output) == false) {
        ANET_LOG(WARN, "failed to encode directPacket:[%p]", directPacket);
        output->stripData(output->getDataLen() - oldLen);
        return false;
    }

    msgSize = output->getDataLen() - oldLen - ANET_DIRECT_PACKET_INFO_LEN;

    // 计算包长度
    header->_dataLen = ANET_DIRECT_PACKET_INFO_LEN + msgSize + payloadSize - PACKET_INFO_LEN;

    // Assert the invalid situation
    DBGASSERT(output->getDataLen() >= oldLen + ANET_DIRECT_PACKET_INFO_LEN);

    // 最终把长度回到buffer中
    if (dataLenOffset >= 0) {
        unsigned char *ptr = (unsigned char *)(output->getData() + dataLenOffset);
        output->fillInt32(ptr, header->_dataLen);
        /* It is possible that the pcode field is updated in packet->encode()
         * logic, so we have to refresh the value below. */
        ptr = (unsigned char *)(output->getData() + pcodeOffset);
        output->fillInt32(ptr, header->_pcode);
        ptr = (unsigned char *)(output->getData() + msgSizeOffset);
        output->fillInt32(ptr, msgSize);
    }

    if (payloadSize > 0) {
        if (payloadSize < DIRECT_WRITE_PAYLOAD_THRESHOLD) {
            output->writeBytes(payload.getAddr(), payloadSize);
        } else {
            payloadToWrite = payload;
        }
    }
    auto channel = directPacket->getChannel();
    if (nullptr != channel && directPacket->hasPlaceholder()) {
        const auto &placeholder = directPacket->getPlaceholder();
        channel->setPlaceholder(placeholder);
        ANET_LOG(DEBUG,
                 "setPlaceholder:[addr:[%p], len:[%d]] with chid:[%lu]",
                 placeholder.getAddr(),
                 placeholder.getLen(),
                 directPacket->getChannelId());
    }
    return true;
}

bool DirectPacketStreamer::processData(DataBuffer *dataBuffer, StreamingContext *context) {
    auto directContext = static_cast<DirectStreamingContext *>(context);
    auto packet = directContext->getDirectPacket();
    if (nullptr == packet) {
        bool brokenFlag = false;
        PacketHeader header;
        DirectPacketHeader directHeader;
        if (!getPacketInfo(dataBuffer, &header, &directHeader, &brokenFlag)) {
            context->setBroken(brokenFlag || context->isEndOfFile());
            return !brokenFlag && !context->isEndOfFile();
        }
        dataBuffer->ensureFree(directHeader._msgSize);
        packet = _directPacketFactory.createDirectPacket(header._pcode);
        assert(packet);
        packet->setPacketHeader(&header);
        packet->setDirectHeader(directHeader);
        context->setPacket(packet);
    }
    auto &directHeader = packet->getDirectHeader();
    if (dataBuffer->getDataLen() < directHeader._msgSize) {
        context->setBroken(context->isEndOfFile());
        return !context->isEndOfFile();
    }

    // DelayDecodePacket is not supported
    if (!packet->decode(dataBuffer, &directHeader)) {
        ControlPacket *cmd = new ControlPacket(ControlPacket::CMD_BAD_PACKET);
        assert(cmd);
        cmd->setPacketHeader(packet->getPacketHeader());
        context->setPacket(cmd);
    }
    context->setCompleted(true);
    return true;
}

bool DirectPacketStreamer::readPayload(
    DataBuffer *input, Socket *socket, ChannelPool *channelPool, DirectStreamingContext *context, int &socketReadLen) {
    auto packet = context->getDirectPacket();
    auto &directHeader = packet->getDirectHeader();
    const auto payloadSize = directHeader._payloadSize;
    if (payloadSize < 0) {
        context->setBroken(true);
        ANET_LOG(WARN, "invalid payloadSize:[%d]", payloadSize);
        return false;
    }
    if (0UL == payloadSize) {
        return true;
    }
    // no cache for placeholder: in case channel timeout
    const auto &placeholder = getPlaceholderFromChannel(channelPool, packet->getChannelId());
    {
        if (nullptr == placeholder.getAddr() || payloadSize > (int32_t)placeholder.getLen()) {
            ANET_LOG(WARN,
                     "packet payload mismatch with target payload addr:[%p], len:[%u] and "
                     "src payload len:[%d]",
                     placeholder.getAddr(),
                     placeholder.getLen(),
                     payloadSize);
            context->setBroken(true);
            return false;
        }
        packet->setPlaceholder(placeholder);
    }
    auto payloadReadOffset = context->getPayloadReadOffset();
    int32_t leftToRead = payloadSize - payloadReadOffset;
    assert(leftToRead > 0);
    const auto bufferLen = input->getDataLen();
    int32_t bufferReadLen = bufferLen >= leftToRead ? leftToRead : bufferLen;
    if (bufferReadLen > 0) { // read from buffer
        ::memcpy(placeholder.getAddr() + payloadReadOffset, input->getData(), bufferReadLen);
        payloadReadOffset += bufferReadLen;
        context->setPayloadReadOffset(payloadReadOffset);
        input->drainData(bufferReadLen);
        leftToRead -= bufferReadLen;
    }
    if (leftToRead == 0) { // finish from buffer
        return true;
    }

    socketReadLen = socket->read(placeholder.getAddr() + payloadReadOffset, leftToRead);
    ANET_LOG(SPAM, "%d bytes read", socketReadLen);
    if (socketReadLen < 0) { // need retry
        return false;
    }
    if (socketReadLen == 0) {
        context->setEndOfFile(true);
        context->setBroken(true);
        ANET_LOG(WARN, "unexpected eof");
        return false;
    }
    payloadReadOffset += socketReadLen;
    context->setPayloadReadOffset(payloadReadOffset);
    return socketReadLen == leftToRead;
}

bool DirectPacketStreamer::getPacketInfo(DataBuffer *input,
                                         PacketHeader *header,
                                         DirectPacketHeader *directHeader,
                                         bool *broken) {
    // if input buffer is not enough for header, we should do nothing.
    if (input->getDataLen() < ANET_DIRECT_PACKET_INFO_LEN) {
        return false;
    }
    if (!DefaultPacketStreamer::getPacketInfo(input, header, broken)) {
        return false;
    }
    // decode direct packet protocol header
    directHeader->_flag = input->readInt32();
    directHeader->_ver = input->readInt32();
    directHeader->_reserved = input->readInt64();
    directHeader->_msgSize = input->readInt32();
    directHeader->_payloadSize = input->readInt32();
    directHeader->_toReceiveSize = input->readInt32();
    if (directHeader->_flag != ANET_DIRECT_PACKET_FLAG || directHeader->_ver != ANET_DIRECT_PACKET_VER) {
        *broken = true;
        ANET_LOG(WARN,
                 "DIRECT_PACKET_FLAG(%08X) VS Packet(%08X) or DIRECT_VER(%08X) VS Packet(%08X)",
                 ANET_DIRECT_PACKET_FLAG,
                 directHeader->_flag,
                 ANET_DIRECT_PACKET_VER,
                 directHeader->_ver);
        return false;
    }
    return true;
}

const DirectPlaceholder &DirectPacketStreamer::getPlaceholderFromChannel(ChannelPool *channelPool, uint64_t channelId) {
    auto channel = channelPool->findChannel(channelId);
    if (nullptr == channel) {
        ANET_LOG(WARN, "channel not found:[%lu]", channelId);
        return kEmptyPlaceholder;
    }
    const auto &placeholder = channel->getPlaceholder();
    ANET_LOG(DEBUG,
             "getPlaceholder from channel:[%lu] with addr:[%p] and len:[%u]",
             channelId,
             placeholder.getAddr(),
             placeholder.getLen());
    return placeholder;
}

Packet *DirectPacketStreamer::decode(DataBuffer *input, PacketHeader *header) {
    ANET_LOG(ERROR, "SHOULD NOT INVOKE DirectPacketStreamer::decode(...)!");
    assert(false);
    return nullptr;
}

} // namespace anet
