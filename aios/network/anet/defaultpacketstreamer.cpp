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
#include "aios/network/anet/streamingcontext.h"
#include "aios/network/anet/defaultpacketstreamer.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/ipacketfactory.h"
#include "aios/network/anet/debug.h"
#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "aios/network/anet/controlpacket.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/ipacketstreamer.h"
#include "aios/network/anet/packet.h"

namespace anet {

const static int32_t max_package_size = 0x40000000; // 1G


DefaultPacketStreamer::DefaultPacketStreamer(IPacketFactory *factory) : IPacketStreamer(factory) {}

DefaultPacketStreamer::~DefaultPacketStreamer() {}

StreamingContext* DefaultPacketStreamer::createContext() {
    return new StreamingContext;
}

bool DefaultPacketStreamer::getPacketInfo(DataBuffer *input, PacketHeader *header, bool *broken) {
    if (_existPacketHeader) {
        if (input->getDataLen() < PACKET_INFO_LEN) {
            return false;
        }
        int flag = input->readInt32();
        header->_chid = input->readInt32();
        header->_pcode = input->readInt32();
        header->_dataLen = input->readInt32();
        if (flag != ANET_PACKET_FLAG || header->_dataLen < 0 || 
            header->_dataLen > max_package_size) { // 1G
            *broken = true;
            ANET_LOG(WARN, "Broken Packet Detected! ANET FLAG(%08X) VS Packet(%08X), Packet Length(%d)",
                     ANET_PACKET_FLAG, flag, header->_dataLen);
            return false;//if broken the head must be wrong
        }
    } else if (input->getDataLen() == 0) {
        return false;
    }
    return true;
}

Packet *DefaultPacketStreamer::decode(DataBuffer *input, PacketHeader *header) {
    ANET_LOG(ERROR, "SHOULD NOT INVOKE DefaultPacketStreamer::decode(...)!");
    assert(false);
    return NULL;
//     Packet *packet = _factory->createPacket(header->_pcode);
//     if (packet != NULL) {
//         if (!packet->decode(input, header)) { 
//             packet->free();
//             packet = NULL;
//         }
//     } else {
//         input->drainData(header->_dataLen);
//     }
//     return packet;
}

bool DefaultPacketStreamer::encode(Packet *packet, DataBuffer *output) {
    PacketHeader *header = packet->getPacketHeader();

    // 为了当encode失败恢复时用
    int oldLen = output->getDataLen();
    // dataLen的位置
    int dataLenOffset = -1;
    int headerSize = 0;

    // pcode offset
    int pcodeOffset = -1;
    if (_existPacketHeader) {
        output->writeInt32(ANET_PACKET_FLAG);
        output->writeInt32(header->_chid);

        pcodeOffset = output->getDataLen();
        output->writeInt32(header->_pcode);

        dataLenOffset = output->getDataLen();
        output->writeInt32(0);
        headerSize = 4 * sizeof(int32_t);
    }
    // 写数据
    if (packet->encode(output) == false) {
        ANET_LOG(ERROR, "encode error");
        output->stripData(output->getDataLen() - oldLen);
        return false;
    }
    // 计算包长度
    header->_dataLen = output->getDataLen() - oldLen - headerSize;

    // Assert the invalid situation
    DBGASSERT(output->getDataLen() >= oldLen + headerSize);

    // 最终把长度回到buffer中
    if (dataLenOffset >= 0) {
        unsigned char *ptr = (unsigned char *)(output->getData() + dataLenOffset);
        output->fillInt32(ptr, header->_dataLen);
        /* It is possible that the pcode field is updated in packet->encode()
         * logic, so we have to refresh the value below. */
        ptr = (unsigned char *)(output->getData() + pcodeOffset);
        output->fillInt32(ptr, header->_pcode);
    }

    return true;
}

bool DefaultPacketStreamer::processData(DataBuffer *dataBuffer,
                                        StreamingContext *context) 
{
    Packet *packet = context->getPacket(); 
    if (NULL == packet) {
        PacketHeader header;
        bool brokenFlag = false;
        //we did not get packet header;
        if (!getPacketInfo(dataBuffer, &header, &brokenFlag)) {
            context->setBroken(brokenFlag);
            return !brokenFlag && !context->isEndOfFile();
        }
        packet = _factory->createPacket(header._pcode);
        assert(packet);
        packet->setPacketHeader(&header);
        context->setPacket(packet);
        dataBuffer->ensureFree(header._dataLen);
    }
    
    PacketHeader *header = packet->getPacketHeader();
    if (dataBuffer->getDataLen() < header->_dataLen) {
        context->setBroken(context->isEndOfFile());
        return !context->isEndOfFile();
    }

    if (!packet->decode(dataBuffer, header)) {
        ControlPacket *cmd = new ControlPacket(ControlPacket::CMD_BAD_PACKET);
        assert(cmd);
        cmd->setPacketHeader(header);
        context->setPacket(cmd);
	/*fix ticket #145*/
	// packet->free(); 
    }
    else 
        DBGASSERT(packet->getDataLen() >= 0);

    context->setCompleted(true);
    return true;
}

}

/////////////
