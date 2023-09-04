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
#include "autil/ExtendHashFunction.h"

#include <iosfwd>

#include "autil/legacy/md5.h"

using namespace std;
using namespace autil::legacy;
namespace autil {

uint32_t ExtendHashFunction::getHashId(const std::string &str) const {
    string hashId = hashString(str);
    return _hasher.GetRangeIndexByKey(hashId);
}

string ExtendHashFunction::hashString(const std::string &str) {
    uint8_t md[16];
    DoMd5(reinterpret_cast<const uint8_t *>(str.c_str()), str.length(), md);

    return extendMd5(md);
}

string ExtendHashFunction::extendMd5(uint8_t md[16]) {
    string hashid;
    // extend 16 bytes hashid to 20 bytes, we apply this extension on the first and second 8 bytes separately,
    // it is sqlengine oriented hashid, each byte ranges from 0 to 127,
    // taking the original hashid as a big number, which is composed of 16 bytes and each ranges from 0 to 255,
    // if we want to save such number in another type of byte, 0-127 range byte, more bytes are needed.
    // that's why the final hashid contains 20 bytes, 4 more than 16 bytes.
    // byte order should be taken into consideration, for simplicity, we adopt __LITTLE_ENDIDAN, which puts significant
    // byte at high address, it is compatible with Intel x86 CPUs but not PowerPC!!!
    uint32_t shiftCount = (8 * 8 + 6) / 7;

    // first 64 bit
    uint64_t value = *(reinterpret_cast<uint64_t *>(md));
    for (uint32_t index = 0; index < shiftCount; ++index) {
        unsigned char *temp = reinterpret_cast<unsigned char *>(&value);
        unsigned char ch = (temp[7]) >> 1;
        hashid.append(1, ch);

        value = value << 7;
    }

    // second 64 bit
    value = *(uint64_t *)(md + 8);
    for (uint32_t index = 0; index < shiftCount; ++index) {
        unsigned char *temp = reinterpret_cast<unsigned char *>(&value);
        unsigned char ch = (temp[7]) >> 1;
        hashid.append(1, ch);

        value = value << 7;
    }

    return hashid;
}

} // namespace autil
