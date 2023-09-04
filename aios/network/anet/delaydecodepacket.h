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
/**
 * File name: delaydecodepacket.h
 * Author: lizhang
 * Create time: 2010-11-07 05:58:28
 * $Id$
 *
 * Description: ***add description here***
 *
 */

#ifndef ANET_DELAYDECODEPACKET_H_
#define ANET_DELAYDECODEPACKET_H_

#include <stdint.h>

#include "aios/network/anet/packet.h"

namespace anet {
class DataBuffer;
class DataBufferSerializable;

/**
 * In some scenarios, we need some additional information to decode the
 * packet. And these information can only be accessed in application's
 * handlePacket(...) callback function. so we need a mechanism to delay
 * some portion of the decoding to application's call back function.
 *
 * Note: DelayDecodePacket should be decoded completely before the
 * callback function return or the data of input buffer will corrupt
 */
class DelayDecodePacket : public Packet {
public:
    DelayDecodePacket();

    virtual ~DelayDecodePacket();

    /**
     * Set content into this packet. if ownContent is true, content will
     * be freed before this packet being freed.
     */
    void setContent(DataBufferSerializable *content, bool ownContent);

    DataBufferSerializable *getContent();
    DataBufferSerializable *stealContent();

    bool encode(DataBuffer *output);

    bool decode(DataBuffer *input, PacketHeader *header);

    // decode the content from 'data' in databuffer
    virtual bool decodeToContent();

    // get the beginning address of databuffer
    char *getData();
    int getEffecitveDataLength();

    void drainData();
    void drainData(int len);
    DataBuffer *getInputDataBuffer();
    int64_t getSpaceUsed();

private:
    // Note: Internal interface for testing. dataBuffer not owned
    // by DelayDecodePacket
    void setInputDataBuffer(DataBuffer *inputDataBuffer);

    void setContentOwnership(bool ownContent) { _ownContent = ownContent; }
    bool ownContent() { return _ownContent; }

    void setNeedDrainDataFlag(bool flag) { _needDrainDataFlag = flag; }
    bool needDrainData() { return _needDrainDataFlag; }

    void clearContent();
    void clearInputDataBuffer();

private:
    bool _ownContent;
    DataBufferSerializable *_content;
    bool _needDrainDataFlag;
    DataBuffer *_inputDataBuffer;
};

} /*end namespace anet*/
#endif /*ANET_DELAYDECODEPACKET_H_*/
