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
#include "autil/codec/EncodeConverter.h"

#include <string>

namespace autil {
namespace codec {
AUTIL_LOG_SETUP(autil::codec, EncodeConverter);

EncodeConverter::EncodeConverter() {}

EncodeConverter::~EncodeConverter() {}

int32_t EncodeConverter::utf8ToUtf16(const char *in, int32_t length, uint16_t *out) {
    int32_t ret = 0;
    const unsigned char *b = reinterpret_cast<const unsigned char *>(in);
    const unsigned char *e = b + length;
    bool error = false;
    while (b < e) {
        if (*b < 128) {
            out[ret++] = *b++;
        } else if ((*b & 0xE0) == 0xC0) {
            uint16_t u = ((*b++) & (0x1F));
            if (b < e && (*b & 0xC0) == 0x80) {
                out[ret++] = (u << 6) | ((*b++) & 0x3F);
            } else {
                error = true;
            }
        } else if ((*b & 0xF0) == 0xE0) {
            uint16_t u = ((*b++) & (0xF));
            if (b < e && (*b & 0xC0) == 0x80) {
                u = (u << 6) | ((*b++) & 0x3F);
                if (b < e && (*b & 0xC0) == 0x80) {
                    out[ret++] = (u << 6) | ((*b++) & 0x3F);
                } else {
                    error = true;
                }
            } else {
                error = true;
            }
        } else {
            error = true;
            b++;
        }
    }

    if (error) {
        AUTIL_LOG(DEBUG, "invalid utf8 [%s]", std::string(in, length).c_str());
    }
    return ret;
}

int32_t EncodeConverter::utf16ToUtf8(const uint16_t *in, int32_t length, char *out) {
    int32_t ret = 0;
    const uint16_t *b = in;
    const uint16_t *e = b + length;
    unsigned char *p = reinterpret_cast<unsigned char *>(out);
    while (b < e) {
        uint16_t u = *b;
        if (u < 0x80) {
            p[ret++] = u;
        } else if (u < 0x800) {
            p[ret++] = (u >> 6) | 0xC0;
            p[ret++] = (u & 0x3F) | 0x80;
        } else {
            p[ret++] = (u >> 12) | 0xE0;
            p[ret++] = ((u >> 6) & 0x3F) | 0x80;
            p[ret++] = (u & 0x3F) | 0x80;
        }
        b++;
    }
    return ret;
}

int32_t EncodeConverter::utf8ToUtf16Len(const char *in, int32_t length) {
    int32_t ret = 0;
    const unsigned char *b = reinterpret_cast<const unsigned char *>(in);
    const unsigned char *e = b + length;
    bool error = false;
    while (b < e) {
        if (*b < 128) {
            ret++;
            b++;
        } else if ((*b & 0xE0) == 0xC0) {
            b++;
            if (b < e && (*b & 0xC0) == 0x80) {
                ret++;
                b++;
            } else {
                error = true;
            }
        } else if ((*b & 0xF0) == 0xE0) {
            b++;
            if (b < e && (*b & 0xC0) == 0x80) {
                b++;
                if (b < e && (*b & 0xC0) == 0x80) {
                    ret++;
                    b++;
                } else {
                    error = true;
                }
            } else {
                error = true;
            }
        } else {
            error = true;
            b++;
        }
    }

    if (error) {
        AUTIL_LOG(DEBUG, "invalid utf8 [%s]", std::string(in, length).c_str());
        return -1;
    }
    return ret;
}

} // namespace codec
} // namespace autil
