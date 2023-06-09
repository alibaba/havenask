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
#ifndef ANET_HTTP_PACKET_STREAMER_H
#define ANET_HTTP_PACKET_STREAMER_H
#include "aios/network/anet/ipacketfactory.h"
#include "aios/network/anet/defaultpacketstreamer.h"
#include "aios/network/anet/httprequestpacket.h"
#include "aios/network/anet/httpresponsepacket.h"

namespace anet {

  class DataBuffer;
  class PacketHeader;

class HttpPacketStreamer : public DefaultPacketStreamer {
public:
    /*
     * 构造函数
     */
    HttpPacketStreamer(IPacketFactory *factory);
    /*
     * 数据包信息的设置
     */
    bool getPacketInfo(DataBuffer *input, PacketHeader *header, bool *broken);
};

/**
 * packet的factory, 缺省的httpd packet factory
 *
 * pcode = 1 是请求包
 * pcode = 0 是响应包
 */
class DefaultHttpPacketFactory : public IPacketFactory {
public:
    Packet *createPacket(int pcode) {
        if (pcode == 1) {
            return new HttpRequestPacket();
        } else {
            return new HttpResponsePacket();
        }
    }
};

}

#endif

