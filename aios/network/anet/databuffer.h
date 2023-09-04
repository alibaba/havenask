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
#ifndef ANET_DATA_BUFFER_H_
#define ANET_DATA_BUFFER_H_
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>

namespace anet {

static const int MAX_BUFFER_SIZE = 65536;

class DataBuffer {
    friend class DataBufferTest_testShrink_Test;
    friend class DataBufferTest_testExpand_Test;
    friend class DataBufferTest_testWriteAndRead_Test;

public:
    static constexpr uint32_t INITIAL_BUFFER_SIZE = 1024;

public:
    DataBuffer() { _pend = _pfree = _pdata = _pstart = NULL; }

    ~DataBuffer() {
        if (_pstart) {
            free(_pstart);
            _pstart = NULL;
        }
    }

    int64_t getSpaceUsed() {
        if (_pstart == NULL) {
            return 0;
        }
        return _pend - _pstart;
    }

    char *getData() { return (char *)_pdata; }

    int getDataLen() { return (_pfree - _pdata); }

    char *getFree() { return (char *)_pfree; }

    int getFreeLen() { return (_pend - _pfree); }

    void drainData(int len) {
        _pdata += len;
        if (_pdata > _pfree) { //~Bug #95
            _pdata = _pfree;
        }

        if (_pdata == _pfree) {
            clear();
        }
    }

    void pourData(int len) {
        assert(_pend - _pfree >= len);
        _pfree += len;
    }

    void stripData(int len) {
        assert(_pfree - _pdata >= len);
        _pfree -= len;
    }

    void clear() { _pdata = _pfree = _pstart; }

    void shrink() {
        if (_pstart == NULL) {
            return;
        }

        int dlen = _pfree - _pdata;
        assert(dlen >= 0);

        if (dlen > 0) {
            unsigned char *newbuf = (unsigned char *)malloc(dlen);
            assert(newbuf != NULL);
            memcpy(newbuf, _pdata, dlen);
            free(_pstart);
            _pdata = _pstart = newbuf;
            _pfree = _pstart + dlen;
            _pend = _pstart + dlen;
        } else {
            free(_pstart);
            _pstart = _pend = _pdata = _pfree = NULL;
        }

        return;
    }

    void writeInt8(int8_t n) {
        expand(1);
        *_pfree++ = (unsigned char)n;
    }

    void writeInt16(int16_t n) {
        expand(2);
        _pfree[1] = (unsigned char)n;
        n >>= 8;
        _pfree[0] = (unsigned char)n;
        _pfree += 2;
    }

    void writeInt32(int32_t n) {
        expand(4);
        _pfree[3] = (unsigned char)n;
        n >>= 8;
        _pfree[2] = (unsigned char)n;
        n >>= 8;
        _pfree[1] = (unsigned char)n;
        n >>= 8;
        _pfree[0] = (unsigned char)n;
        _pfree += 4;
    }

    void writeInt64(int64_t n) {
        expand(8);
        _pfree[7] = (unsigned char)n;
        n >>= 8;
        _pfree[6] = (unsigned char)n;
        n >>= 8;
        _pfree[5] = (unsigned char)n;
        n >>= 8;
        _pfree[4] = (unsigned char)n;
        n >>= 8;
        _pfree[3] = (unsigned char)n;
        n >>= 8;
        _pfree[2] = (unsigned char)n;
        n >>= 8;
        _pfree[1] = (unsigned char)n;
        n >>= 8;
        _pfree[0] = (unsigned char)n;
        _pfree += 8;
    }

    void writeBytes(const void *src, int len) {
        expand(len);
        memcpy(_pfree, src, len);
        _pfree += len;
    }

    void fillInt32(unsigned char *dst, uint32_t n) {
        dst[3] = (unsigned char)n;
        n >>= 8;
        dst[2] = (unsigned char)n;
        n >>= 8;
        dst[1] = (unsigned char)n;
        n >>= 8;
        dst[0] = (unsigned char)n;
    }

    int8_t readInt8() { return (*_pdata++); }

    int16_t readInt16() {
        // not portable!
        // need some macros to determine where big-endian or little endian
        int16_t n = _pdata[0];
        n <<= 8;
        n |= _pdata[1];
        _pdata += 2;
        return n;
    }

    int32_t readInt32() {
        int32_t n = _pdata[0];
        n <<= 8;
        n |= _pdata[1];
        n <<= 8;
        n |= _pdata[2];
        n <<= 8;
        n |= _pdata[3];
        _pdata += 4;
        return n;
    }

    int64_t readInt64() {
        int64_t n = _pdata[0];
        n <<= 8;
        n |= _pdata[1];
        n <<= 8;
        n |= _pdata[2];
        n <<= 8;
        n |= _pdata[3];
        n <<= 8;
        n |= _pdata[4];
        n <<= 8;
        n |= _pdata[5];
        n <<= 8;
        n |= _pdata[6];
        n <<= 8;
        n |= _pdata[7];
        _pdata += 8;
        return n;
    }

    void readBytes(void *dst, int len) {
        memcpy(dst, _pdata, len);
        _pdata += len;
    }

    void ensureFree(int len) { expand(len); }

    int findBytes(const char *findstr, int len) {
        int dLen = _pfree - _pdata - len + 1;
        for (int i = 0; i < dLen; i++) {
            if (_pdata[i] == findstr[0] && memcmp(_pdata + i, findstr, len) == 0) {
                return i;
            }
        }
        return -1;
    }

    template <typename T>
    inline void read(T &value) {
        readBytes(&value, sizeof(value));
    }

    template <typename T>
    inline void write(const T &value) {
        writeBytes(&value, sizeof(value));
    }

private:
    inline void expand(int need) {
        if (_pstart == NULL) {
            int len = INITIAL_BUFFER_SIZE;
            while (len < need)
                len <<= 1;
            _pfree = _pdata = _pstart = (unsigned char *)malloc(len);
            assert(_pstart);
            _pend = _pstart + len;
        } else if (_pend - _pfree < need) { // not enough free space
            int flen = (_pend - _pfree) + (_pdata - _pstart);
            int dlen = _pfree - _pdata;

            if (flen < need || flen * 4 < dlen) {
                float bufsize = (_pend - _pstart) * 2;
                while (bufsize - dlen < need)
                    bufsize *= 1.5;

                unsigned char *newbuf = (unsigned char *)malloc((int)bufsize);
                assert(newbuf != NULL);
                if (dlen > 0) {
                    memcpy(newbuf, _pdata, dlen);
                }
                free(_pstart);

                _pdata = _pstart = newbuf;
                _pfree = _pstart + dlen;
                _pend = _pstart + (int)bufsize;
            } else {
                memmove(_pstart, _pdata, dlen);
                _pfree = _pstart + dlen;
                _pdata = _pstart;
            }
        }
    }

private:
    unsigned char *_pstart; // buffer start address
    unsigned char *_pend;   // buffer end address
    unsigned char *_pfree;  // end of data
    unsigned char *_pdata;  // begin of data
};

#define SPECIALIZE_READ_WRITE_FOR_INT(w)                                                                               \
    template <>                                                                                                        \
    inline void DataBuffer::read(int##w##_t &value) {                                                                  \
        value = readInt##w();                                                                                          \
    }                                                                                                                  \
                                                                                                                       \
    template <>                                                                                                        \
    inline void DataBuffer::write(const int##w##_t &value) {                                                           \
        writeInt##w(value);                                                                                            \
    }                                                                                                                  \
                                                                                                                       \
    template <>                                                                                                        \
    inline void DataBuffer::read(uint##w##_t &value) {                                                                 \
        value = readInt##w();                                                                                          \
    }                                                                                                                  \
                                                                                                                       \
    template <>                                                                                                        \
    inline void DataBuffer::write(const uint##w##_t &value) {                                                          \
        writeInt##w(value);                                                                                            \
    }

SPECIALIZE_READ_WRITE_FOR_INT(8);
SPECIALIZE_READ_WRITE_FOR_INT(16);
SPECIALIZE_READ_WRITE_FOR_INT(32);
SPECIALIZE_READ_WRITE_FOR_INT(64);

// template <> inline void DataBuffer::read(int8_t &value) {value = readInt8();}
// template <> inline void DataBuffer::read(int16_t &value) {value = readInt16();}
// template <> inline void DataBuffer::read(int32_t &value) {value = readInt32();}
// template <> inline void DataBuffer::read(int64_t &value) {value = readInt64();}

// template <> inline void DataBuffer::write(const int8_t &value) {writeInt8(value);}
// template <> inline void DataBuffer::write(const int16_t &value) {writeInt16(value);}
// template <> inline void DataBuffer::write(const int32_t &value) {writeInt32(value);}
// template <> inline void DataBuffer::write(const int64_t &value) {writeInt64(value);}

// template <> inline void DataBuffer::read(uint8_t &value) {value = readInt8();}
// template <> inline void DataBuffer::read(uint16_t &value) {value = readInt16();}
// template <> inline void DataBuffer::read(uint32_t &value) {value = readInt32();}
// template <> inline void DataBuffer::read(uint64_t &value) {value = readInt64();}

// template <> inline void DataBuffer::write(const uint8_t &value) {writeInt8(value);}
// template <> inline void DataBuffer::write(const uint16_t &value) {writeInt16(value);}
// template <> inline void DataBuffer::write(const uint32_t &value) {writeInt32(value);}
// template <> inline void DataBuffer::write(const uint64_t &value) {writeInt64(value);}

template <>
inline void DataBuffer::read(std::string &value) {
    int32_t size;
    read(size);
    value = std::string(getData(), size);
    drainData(size);
}

template <>
inline void DataBuffer::write(const std::string &value) {
    write((int32_t)value.length()); // use int32_t lengh to be independant to arch
    writeBytes((char *)(value.data()), value.length());
}

} // namespace anet

#endif /*DATABUFFER_H_*/
