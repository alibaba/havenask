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
#include "aios/network/anet/directpacket.h"
#include "aios/network/anet/log.h"

#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/ilogger.h"

namespace anet {

bool DirectPacket::decode(DataBuffer *input, DirectPacketHeader *header) {
    if (input->getDataLen() < header->_msgSize) {
        ANET_LOG(ERROR, "invalid input to decode with dataLen:[%d] and header's msgSize:[%d]",
                 input->getDataLen(), header->_msgSize);
        return false;
    }
    bool rc = setBody(input->getData(), header->_msgSize);
    input->drainData(header->_msgSize);
    return rc;
}

}  // namespace anet
