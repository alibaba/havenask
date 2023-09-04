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
#include "autil/codec/CodeConverter.h"

#include <cstddef>
#include <cstring>

#include "autil/CommonMacros.h"
#include "autil/codec/CodeConverterTBL.h"

using namespace std;
namespace autil {
namespace codec {

U16CHAR CodeConverter::convertGBKToUTF16(U16CHAR c) {
    U16CHAR ret = 0;
    if (c >= GBK_START && c <= GBK_END) {
        ret = gbk2unicode[c - GBK_START];
    }
    if (ret == 0)
        return defUniChar;
    return ret;
}

U16CHAR CodeConverter::convertUTF16ToGBK(U16CHAR c) {
    if (unicode2gbk[c - 1] != 0)
        return unicode2gbk[c - 1];
    return defGbkChar;
}

U16CHAR CodeConverter::convertBig5ToUTF16(U16CHAR c) {
    U16CHAR ret = 0;
    if (c >= BIG_START && c <= BIG_END) {
        ret = big2unicode[c - BIG_START];
    }
    if (ret == 0)
        return defUniChar;
    return ret;
}

U16CHAR CodeConverter::convertUTF16ToBig5(U16CHAR c) {
    if (unicode2big[c - 1] != 0)
        return unicode2big[c - 1];
    return defBigChar;
}

int32_t CodeConverter::convertGBKToUTF16(U8CHAR *src, uint32_t len, U16CHAR *dst, uint32_t buf_len) {
    uint32_t odx = 0;
    for (uint32_t i = 0; i < len; i++) {
        if (odx >= buf_len) {
            break;
        }
        if (src[i] < 0x80) {
            dst[odx] = src[i];
        } else {
            dst[odx] = CodeConverter::convertGBKToUTF16((src[i] << 8) | src[i + 1]);
            i++;
        }
        odx++;
    }
    return odx;
}

int32_t CodeConverter::convertUTF16ToGBK(U16CHAR *src, uint32_t len, U8CHAR *dst, uint32_t buf_len) {
    U16CHAR val;
    uint32_t odx = 0;
    for (uint32_t i = 0; i < len; i++) {
        val = CodeConverter::convertUTF16ToGBK(src[i]);
        if (val < 0x80) {
            if (odx >= buf_len)
                break;
            dst[odx++] = val;
        } else {
            if (odx + 1 >= buf_len)
                break;
            dst[odx++] = val >> 8;
            dst[odx++] = val & 0xFF;
        }
    }
    return odx;
}

int32_t CodeConverter::convertBig5ToUTF16(U8CHAR *src, uint32_t len, U16CHAR *dst, uint32_t buf_len) {
    uint32_t odx = 0;
    for (uint32_t i = 0; i < len; i++) {
        if (odx >= buf_len) {
            break;
        }
        if (src[i] < 0x80) {
            dst[odx++] = src[i];
        } else {
            dst[odx++] = CodeConverter::convertBig5ToUTF16((src[i] << 8) | src[i + 1]);
            i++;
        }
    }
    return odx;
}

int32_t CodeConverter::convertUTF16ToBig5(U16CHAR *src, uint32_t len, U8CHAR *dst, uint32_t buf_len) {
    U16CHAR val;
    uint32_t odx = 0;
    for (uint32_t i = 0; i < len; i++) {
        val = CodeConverter::convertUTF16ToBig5(src[i]);
        if (val < 0x80) {
            if (odx >= buf_len)
                break;
            dst[odx++] = val;
        } else {
            if (odx + 3 >= buf_len)
                break;
            dst[odx++] = val >> 8;
            dst[odx++] = val & 0xFF;
        }
    }
    return odx;
}

/********* utf8 <=> utf16 **********/
#define UNI_SUR_HIGH_START (uint32_t)0xD800
#define UNI_SUR_HIGH_END (uint32_t)0xDBFF
#define UNI_SUR_LOW_START (uint32_t)0xDC00
#define UNI_SUR_LOW_END (uint32_t)0xDFFF

static const int32_t halfShift = 10; /* used for shifting by 10 bits */
static const uint32_t halfBase = 0x0010000UL;
static const char trailingBytesForUTF8[256] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5};
static const uint32_t offsetsFromUTF8[6] = {
    0x00000000UL, 0x00003080UL, 0x000E2080UL, 0x03C82080UL, 0xFA082080UL, 0x82082080UL};
static const uint8_t firstByteMark[7] = {0x00, 0x00, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC};

static bool isLegalUTF8(const U8CHAR *source, int32_t length) {
    U8CHAR a;
    const U8CHAR *srcptr = source + length;
    switch (length) {
    default:
        return false;
    /* Everything else falls through when "true"... */
    case 4:
        if ((a = (*--srcptr)) < 0x80 || a > 0xBF)
            return false;
    case 3:
        if ((a = (*--srcptr)) < 0x80 || a > 0xBF)
            return false;
    case 2:
        if ((a = (*--srcptr)) > 0xBF)
            return false;
        switch (*source) {
        /* no fall-through in this inner switch */
        case 0xE0:
            if (a < 0xA0)
                return false;
            break;
        case 0xF0:
            if (a < 0x90)
                return false;
            break;
        case 0xF4:
            if (a > 0x8F)
                return false;
            break;
        default:
            if (a < 0x80)
                return false;
        }
    case 1:
        if (*source >= 0x80 && *source < 0xC2)
            return false;
        if (*source > 0xF4)
            return false;
    }
    return true;
}

int32_t CodeConverter::convertUTF8ToUTF16(U8CHAR *src, uint32_t src_len, U16CHAR *dest, uint32_t dest_len) {
    U8CHAR *srcEnd = src + src_len;
    U16CHAR *destStart = dest;
    U16CHAR *destEnd = dest + dest_len;
    while (src < srcEnd && dest < destEnd) {
        uint64_t ch = *src;
        U16CHAR extraBytesToRead = trailingBytesForUTF8[ch];
        if (src + extraBytesToRead >= srcEnd) {
            break;
        }
        if (isLegalUTF8(src, extraBytesToRead + 1)) {
            ch = 0;
            switch (extraBytesToRead) {
            case 3:
                ch += *src++;
                ch <<= 6;
            case 2:
                ch += *src++;
                ch <<= 6;
            case 1:
                ch += *src++;
                ch <<= 6;
            case 0:
                ch += *src++;
            }
            ch -= offsetsFromUTF8[extraBytesToRead];
        } else {
            return -1;
        }
        if (ch < 0x10000) {
            *dest++ = ch;
        } else if (dest + 2 < destEnd) {
            *dest++ = 0xD800 | (((ch) >> 10) - 0x0040);
            *dest++ = 0xDC00 | ((ch)&0x03FF);
        } else {
            return -1;
        }
    }
    return dest - destStart;
}

int32_t CodeConverter::convertUTF16ToUTF8(U16CHAR *src, uint32_t srcLen, U8CHAR *dest, uint32_t destLen) {
    const U16CHAR *srcEnd = src + srcLen;
    U8CHAR *destStart = dest;
    U8CHAR *destEnd = dest + destLen;

    while (src < srcEnd) {
        uint32_t ch;
        U16CHAR bytesToWrite = 0;
        const uint32_t byteMask = 0xBF;
        const uint32_t byteMark = 0x80;

        ch = *src++;
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END && src < srcEnd) {
            uint32_t ch2 = *src;
            if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) {
                ch = ((ch - UNI_SUR_HIGH_START) << halfShift) + (ch2 - UNI_SUR_LOW_START) + halfBase;
                ++src;
            } else {
                return -1;
            }
        } else if (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END) {
            return -1;
        }
        /* Figure out how many bytes the result will require */
        if (ch < (uint32_t)0x80) {
            bytesToWrite = 1;
        } else if (ch < (uint32_t)0x800) {
            bytesToWrite = 2;
        } else if (ch < (uint32_t)0x10000) {
            bytesToWrite = 3;
        } else if (ch < (uint32_t)0x200000) {
            bytesToWrite = 4;
        } else {
            return -1;
        }
        dest += bytesToWrite;
        if (dest < destEnd) {
            switch (bytesToWrite) { /* note: everything falls through. */
            case 4:
                *--dest = (ch | byteMark) & byteMask;
                ch >>= 6;
            case 3:
                *--dest = (ch | byteMark) & byteMask;
                ch >>= 6;
            case 2:
                *--dest = (ch | byteMark) & byteMask;
                ch >>= 6;
            case 1:
                *--dest = ch | firstByteMark[bytesToWrite];
            }
            dest += bytesToWrite;

        } else {
            return -1;
        }
    }
    return dest - destStart;
}
#undef UNI_SUR_HIGH_START
#undef UNI_SUR_HIGH_END
#undef UNI_SUR_LOW_START
#undef UNI_SUR_LOW_END

static const uint32_t s_utf8_head_mask[] = {
    0x00, // unused
    0x00, // unused
    0xC0, // 2 bytes 110XXXXX
    0xE0, // 3 bytes 1110XXXX
    0xF0, // 4 bytes 11110XXX
    0xF8, // 5 bytes 111110XX
    0xFC, // 6 bytes 1111110X
};

static inline int32_t unicodeCharToUTF8(uint16_t value, char *buf, int32_t buf_size) {
    if (buf_size < 1) {
        return -1;
    }
    char word[6];
    word[sizeof(word) - 1] = '\0';
    if (value < 0x80) { // deal with one byte
        buf[0] = value;
        return 1;
    }

    int32_t j = sizeof(word) - 2; // deal with multi byte word
    int32_t len = 1;
    while (j > 0) {
        U8CHAR ch = value & 0x3f;
        word[j--] = ch | 0x80;
        value = value >> 6;
        ++len;
        if (value < 0x20) {
            break;
        }
    }
    word[j] = value | s_utf8_head_mask[len];
    if (unlikely(len > buf_size)) {
        return -1;
    }
    memcpy(buf, word + j, len);
    return len;
}

int32_t CodeConverter::convertUnicodeToUTF8(const uint16_t *src, int32_t src_len, char *dest, int32_t buf_len) {
    if (NULL == src || NULL == dest || buf_len < src_len) {
        return -1;
    }
    int32_t dcnt = 0;
    for (int32_t i = 0; i < src_len; ++i) {
        int32_t len = unicodeCharToUTF8(src[i], dest + dcnt, buf_len - dcnt);
        if (unlikely(len < 0)) {
            return -1;
        }
        dcnt += len;
    }
    dest[dcnt] = '\0';
    return dcnt;
}

} // namespace codec
} // namespace autil
