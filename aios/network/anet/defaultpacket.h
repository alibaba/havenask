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
 * File name: defaultpacket.h
 * Author: zhangli
 * Create time: 2008-12-25 11:10:50
 * $Id$
 * 
 * Description: ***add description here***
 * 
 */

#ifndef ANET_DEFAULTPACKET_H_
#define ANET_DEFAULTPACKET_H_
#include "aios/network/anet/packet.h"
#include <stddef.h>
#include <stdint.h>

namespace anet {
class DataBuffer;

class DefaultPacket : public Packet
{
public:
    DefaultPacket();
    ~DefaultPacket();
    
    bool setBody(const char *, size_t);
    bool appendBody(const char *, size_t);
    const char* getBody(size_t&) const; 
    char* getBody(size_t&); 
    const char* getBody() const;
    char* getBody();

    bool encode(DataBuffer *output);
    bool decode(DataBuffer *input, PacketHeader *header); 

    int64_t getSpaceUsed();
    size_t getBodyLen() const;
    bool setCapacity(size_t size);
    size_t getCapacity();

protected:
    char *_body;
    size_t _bodyLength;
    size_t _capacity;
};

}/*end namespace anet*/
#endif /*ANET_DEFAULTPACKET_H_*/
