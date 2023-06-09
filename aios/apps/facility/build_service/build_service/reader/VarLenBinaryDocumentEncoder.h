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
#ifndef ISEARCH_BS_VARLENBINARYDOCUMENTENCODER_H
#define ISEARCH_BS_VARLENBINARYDOCUMENTENCODER_H

#include <string>

namespace build_service { namespace reader {

class VarLenBinaryDocumentEncoder
{
public:
    static const uint32_t MAGIC_HEADER = 0xF1F2F3F4;

public:
    static std::string encode(const std::string& data)
    {
        uint32_t value = (uint32_t)data.size();
        int encodeLen = 1;
        while (value > 0x7F) {
            ++encodeLen;
            value >>= 7;
        }
        std::string ret;
        ret.resize(encodeLen + sizeof(uint32_t) + data.size());
        encode((char*)data.data(), data.size(), (char*)ret.data(), ret.size());
        return ret;
    }

    /*
       1. return encoded data length when Encode success,
       2. return 0 when Encode fail
       3. memory format : magic header + vbyte length + data
    */
    static uint32_t encode(char* input, uint32_t inputLen, char* output, uint32_t outputLen)
    {
        if (!input || !output) {
            return 0;
        }
        uint32_t value = inputLen;
        char buf[5];
        int cursor = 0;
        while (value > 0x7F) {
            buf[cursor++] = 0x80 | (value & 0x7F);
            value >>= 7;
        }
        buf[cursor++] = value & 0x7F;

        uint32_t encodeLen = cursor + sizeof(MAGIC_HEADER) + inputLen;
        if (outputLen < encodeLen) {
            return 0;
        }
        *((uint32_t*)output) = MAGIC_HEADER;
        output += sizeof(MAGIC_HEADER);
        memcpy(output, buf, cursor);
        output += cursor;
        memcpy(output, input, inputLen);
        return encodeLen;
    }
};

}} // namespace build_service::reader

#endif // ISEARCH_BS_VARLENBINARYDOCUMENTENCODER_H
