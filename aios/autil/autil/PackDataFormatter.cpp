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
#include "autil/PackDataFormatter.h"

#include <limits>

namespace autil {

void PackDataFormatter::setVarLenOffset(const PackOffset& pOffset, char* baseAddr, size_t dataCursor)
{
    size_t offset = pOffset.getOffset();
    size_t varIndex = pOffset.getVarLenFieldIdx();
    size_t varNum = pOffset.getVarLenFieldNum();
    if (pOffset.isImpactFormat()) {
        if (varIndex == 0) { // no need store offset value
            return;
        }
        assert(varNum > 1);
        size_t offsetLen = baseAddr[offset];
        char* offsetAddr = baseAddr + offset + sizeof(char);
        size_t offsetEndCursor = offset + sizeof(char) + offsetLen * (varNum - 1);
        assert(dataCursor >= offsetEndCursor);
        size_t offsetValue = (dataCursor - offsetEndCursor);

        switch (offsetLen) {
        case 1: {
            assert(offsetValue <= std::numeric_limits<uint8_t>::max());
            uint8_t* addr = (uint8_t*)offsetAddr;
            addr[varIndex - 1] = (uint8_t)offsetValue;
            break;
        }
        case 2: {
            assert(offsetValue <= std::numeric_limits<uint16_t>::max());
            uint16_t* addr = (uint16_t*)offsetAddr;
            addr[varIndex - 1] = (uint16_t)offsetValue;
            break;
        }
        case 4: {
            assert(offsetValue <= std::numeric_limits<uint32_t>::max());
            uint32_t* addr = (uint32_t*)offsetAddr;
            addr[varIndex - 1] = (uint32_t)offsetValue;
            break;
        }
        default:
            assert(false);
        }
    } else {
        assert(dataCursor >= offset);
        typedef int64_t MultiValueOffsetType;
        MultiValueOffsetType* pOffset = (MultiValueOffsetType*)(baseAddr + offset);
        *pOffset = (MultiValueOffsetType)(dataCursor - offset);
    }
}

}
