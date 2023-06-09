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
#include "aios/network/anet/httppacketstreamer.h"

#include <string.h>

#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/defaultpacketstreamer.h"
#include "aios/network/anet/packet.h"

namespace anet {
class IPacketFactory;
/*
 * 构造函数
 */

HttpPacketStreamer::HttpPacketStreamer(IPacketFactory *factory) : DefaultPacketStreamer(factory) {
    _existPacketHeader = false; // 不要输出头信息
}

/*
 * 数据包信息的设置
 */
bool HttpPacketStreamer::getPacketInfo(DataBuffer *input, PacketHeader *header, bool *broken) {
    if (input->getDataLen() == 0) {
        return false;
    }
    char *data = input->getData();
    int cmplen = input->getDataLen();
    if (cmplen > 4) cmplen = 4;
    if (memcmp(data, "GET ", cmplen)) {
        *broken = true;
        return false;
    }

    int nr = input->findBytes("\r\n\r\n", 4);
    if (nr < 0) {
        return false;
    }

    header->_pcode = 1;
    header->_chid = 0;
    header->_dataLen = nr + 4;
    return true;
}

}



