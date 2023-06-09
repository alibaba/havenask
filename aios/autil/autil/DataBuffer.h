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
#pragma once

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <unordered_map>
#include <memory>
#include <set>
#include <unordered_set>
#include <string>
#include <type_traits>
#include <vector>
#include <iterator>

#include "autil/Span.h"
#include "autil/mem_pool/Pool.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "autil/MultiValueFormatter.h"
#include "autil/legacy/exception.h"
#include "autil/CommonMacros.h"

namespace autil {

struct SimpleType {};
struct SerializableType {};
// IWYU pragma: no_forward_declare SerializableTypeTraits
template<typename T, bool =
         std::is_arithmetic<T>::value ||
         std::is_enum<T>::value>
struct SerializableTypeTraits;

template <typename T>
struct SerializableTypeTraits<T, false> {
    typedef SerializableType Type;
};

template<typename T>
struct SerializableTypeTraits<T, true> {
    typedef SimpleType Type;
};

#define DECLARE_SIMPLE_TYPE(T)                           \
    template<> struct SerializableTypeTraits<T, false> { \
        typedef SimpleType Type;                         \
    }


DECLARE_SIMPLE_TYPE(uint128_t);

#define AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(data, end, len)    \
    if (unlikely((len) < 0 || data + (len) > end)) {            \
        dataBufferCorrupted();                                  \
    }

#define AUTIL_DATABUFFER_VALID_AND_MOVE_BACKWARD(data, end, len)        \
    if (unlikely((len) < 0 || data + (len) > end)) {                    \
        return;                                                         \
    }                                                                   \
    end -= len;

class DataBufferCorruptedException : public legacy::ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(DataBufferCorruptedException, ExceptionBase);
};

struct SectionHeader {
    uint32_t checkSum:32;
    uint16_t metaLen:16;
    uint16_t reserve:16;
};

class DataBuffer; // IWYU pragma: keep
class SectionMeta {
public:
    struct SectionInfo {
        SectionInfo() : name(""), start(0u), length(0u) {}
        SectionInfo(const std::string &n, const uint32_t s,
                    const uint32_t l) : name(n), start(s), length(l) {}
        void serialize(DataBuffer &dataBuffer) const;
        void deserialize(DataBuffer &dataBuffer);
        std::string name;
        uint32_t start;
        uint32_t length;
    };

public:
    SectionMeta() {
    }
    void serialize(DataBuffer &dataBuffer) const;
    void deserialize(DataBuffer &dataBuffer);
    void addSectionInfo(const std::string& secName, const uint32_t offset,
                       const uint32_t length)
    {
        _sectionInfoVec.push_back(SectionInfo(secName, offset, length));
    }
    const std::vector<SectionInfo>& getSectionInfoVec() const{
        return _sectionInfoVec;
    }
public:
    bool validate() {return true;}
private:
    SectionMeta(const SectionMeta&);

private:
    std::vector<SectionInfo> _sectionInfoVec;
};

class DataBuffer {
public:
    static const int DEFAUTL_DATA_BUFFER_SIZE = 1024 * 64; //64k
    static const int MAX_VARINT64_BYTES = 10;
    static const int MAX_VARINT32_BYTES = 5;
    static const uint32_t SECTION_FLAG = 0x9e3779b9;
    //phi = (1 + sqrt(5)) / 2; 2^32 / phi = 0x9e3779b9
public:
    DataBuffer(int len = DEFAUTL_DATA_BUFFER_SIZE,
               autil::mem_pool::Pool *pool = NULL)
    {
        init(len, pool);
    }

    DataBuffer(void* data, size_t dataLen, mem_pool::Pool *pool = NULL) {
        init(data, dataLen, pool);
    }

    ~DataBuffer() {
        reset();
    }

private:
    unsigned char* allocateMemory(const size_t memLen);
    void init(int len, mem_pool::Pool *pool);
    void init(void* data, size_t dataLen, mem_pool::Pool *pool);
    void reset();
    void buildSectionMeta(SectionMeta &secMeta, size_t &totalDataLen);
    void fillData(unsigned char *pbuf);
    void fillSectionData(unsigned char *pbuf);
    void fillSectionMeta(unsigned char *pbuf, const DataBuffer &metaBuffer);
    void fillSectionHeader(unsigned char *pbuf, const DataBuffer &metaBuffer);
    void fillSectionFlag(unsigned char *pbuf);
    bool checkSectionFlag(const unsigned char* pbuf);
    bool extractAndCheckSectionHeader(SectionHeader &header,
                                  const unsigned char* buf);
    bool extractAndCheckSectionMeta(SectionMeta &secMeta, size_t metaLen,
                                const unsigned char *metaData);
    void extractSectionBuffer(const SectionMeta &secMeta, size_t &sectionDataLen);
    void initByString(const std::string &str, mem_pool::Pool *pool, bool needCopy);

public:
    mem_pool::Pool *getPool() const {
        return _pool;
    }

    char* getStart() const {
        return (char*)_pstart;
    }

    char* getEnd() const {
        return (char*)_pend;
    }

    char* getData() const {
        return (char*)_pdata;
    }

    int getDataLen() const {
        return (_pfree - _pdata);
    }

    char* getFree() {
        return (char*)_pfree;
    }

    size_t getFreeLen() const {
        return (_pend - _pfree);
    }
    void skipData(uint32_t len) {
        _pdata += len;
    }

    void clear() {
        _pdata = _pfree = _pstart;
    }

    void serializeToStringWithSection(std::string &str);

    void deserializeFromStringWithSection(const std::string &str,
            mem_pool::Pool *pool = NULL,
            bool needCopy=true);

    DataBuffer* findSectionBuffer(const std::string& name);
    DataBuffer* declareSectionBuffer(const std::string& name);

    template<typename T>
    inline void write(const T &value) {
        _write(value, typename SerializableTypeTraits<T>::Type());
    }

    template<typename T>
    inline void read(T &value) {
        _read(value, typename SerializableTypeTraits<T>::Type());
    }

    template<typename T, typename Alloc>
    inline void read(T &value, Alloc *allocator) {
        _read(value, typename SerializableTypeTraits<T>::Type(), allocator);
    }

    template<typename MultiT>
    inline void read(MultiValueType<MultiT> &value);

    template<typename MultiT, typename Alloc>
    inline void read(MultiValueType<MultiT> &value, Alloc *allocator);

    template<typename MultiT>
    inline void write(const MultiValueType<MultiT> &value);

    template <typename Alloc>
    inline void read(StringView &value, Alloc *allocator) {
        uint32_t len = 0;
        read(len);
        char *data = static_cast<char*>(allocator->allocate(len));
        assert(data != nullptr);
        readBytes(data, len);
        makeConstString(value, data, len);
    }

    template<typename T>
    inline void write(T* const &p) {
        write(static_cast<T const * const>(p));
    }

    template<typename T>
    inline void write(T const * const &p) {
        bool isNull = p;
        write(isNull);
        if (isNull) {
            write(*p);
        }
    }

    template<typename T>
    inline void read(T * &p) {
#if __cplusplus >= 201103L
        std::unique_ptr<T> ptr;
#else
        std::auto_ptr<T> ptr;
#endif
        bool isNull;
        read(isNull);
        if (isNull) {
            ptr.reset(new T());
            read(*ptr);
            p = ptr.release();
        } else {
            p = NULL;
        }
    }

    template<typename T, typename Alloc>
    inline void read(T * &p, Alloc *allocator) {
#if __cplusplus >= 201103L
        std::unique_ptr<T> ptr;
#else
        std::auto_ptr<T> ptr;
#endif
        bool isNull;
        read(isNull);
        if (isNull) {
            ptr.reset(new T());
            read(*ptr, allocator);
            p = ptr.release();
        } else {
            p = NULL;
        }
    }

#define SERIALIZE_PTR(PtrType)                                  \
    template<typename T>                                        \
    inline void write(const PtrType<T> &ptr) {                  \
        write(ptr.get());                                       \
    }                                                           \
                                                                \
    template<typename T>                                        \
    inline void read(PtrType<T> &ptr) {                         \
        T *p;                                                   \
        read(p);                                                \
        ptr.reset(p);                                           \
    }                                                           \
                                                                \
    template<typename T, typename Alloc>                        \
    inline void read(PtrType<T> &ptr, Alloc *allocator) {       \
        T *p;                                                   \
        read(p, allocator);                                     \
        ptr.reset(p);                                           \
    }                                                           \

SERIALIZE_PTR(std::shared_ptr)
SERIALIZE_PTR(std::unique_ptr)

    template<typename T>
    inline void write(const std::vector<T> &value) {
        _write(value, typename SerializableTypeTraits<T>::Type());
    }

    template<typename T>
    inline void read(std::vector<T> &value) {
        _read(value, typename SerializableTypeTraits<T>::Type());
    }

    template<typename T, typename Alloc>
    inline void read(std::vector<T> &value, Alloc *allocator) {
        _read(value, typename SerializableTypeTraits<T>::Type(), allocator);
    }

#define SERIALIZE_MAP(ContainerType)                                    \
    template<typename K, typename V>                                    \
    void write(const ContainerType<K, V> &value) {                      \
        uint32_t size = value.size();                                   \
        write(size);                                                    \
        for (typename ContainerType<K, V>::const_iterator it = value.begin(); \
             it != value.end(); ++it)                                   \
        {                                                               \
            write(it->first);                                           \
            write(it->second);                                          \
        }                                                               \
    }                                                                   \
    template<typename K, typename V>                                    \
    void read(ContainerType<K, V> &value) {                             \
        uint32_t size = 0;                                              \
        read(size);                                                     \
        value.clear();                                                  \
        for (uint32_t i = 0; i < size; ++i)                             \
        {                                                               \
            K key{};                                                      \
            assert(value.find(key) == value.end());                     \
            read(key);                                                  \
            read(value[key]);                                           \
        }                                                               \
    }

    SERIALIZE_MAP(std::map);
    SERIALIZE_MAP(std::unordered_map);

#define SERIALIZE_SET(ContainerType)                                    \
    template<typename T>                                                \
    void write(const ContainerType<T> &value) {                         \
        uint32_t size = value.size();                                   \
        write(size);                                                    \
        for (typename ContainerType<T>::const_iterator it = value.begin(); \
             it != value.end(); ++it)                                   \
        {                                                               \
            write(*it);                                                 \
        }                                                               \
    }                                                                   \
    template<typename T>                                                \
    void read(ContainerType<T> &value) {                                \
        uint32_t size = 0;                                              \
        read(size);                                                     \
        value.clear();                                                  \
        for (uint32_t i = 0; i < size; ++i)                             \
        {                                                               \
            T key;                                                      \
            read(key);                                                  \
            assert(value.find(key) == value.end());                     \
            value.insert(key);                                          \
        }                                                               \
    }

    SERIALIZE_SET(std::set);
    SERIALIZE_SET(std::unordered_set);

    template<typename F, typename S>
    void write(const std::pair<F, S> &value) {
        write(value.first);
        write(value.second);
    }

    template<typename F, typename S>
    void read(std::pair<F, S> &value) {
        read(value.first);
        read(value.second);
    }

    void *writeNoCopy(int len) {
        expand(len);
        void *ret = _pfree;
        _pfree += len;
        return ret;
    }

    const void *readNoCopy(int len) {
        void *ret = _pdata;
        AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(_pdata, _pend, len);
        _pdata += len;
        return ret;
    }

public:
    void writeBytes(const void *src, int len) {
        expand(len);
        memcpy(_pfree, src, len);
        _pfree += len;
    }

    void readBytes(void *dst, int len) {
        AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(_pdata, _pend, len);
        memcpy(dst, _pdata, len);
        _pdata += len;
    }

    int findBytes(const char *findstr, int len) {
        int dLen = _pfree - _pdata - len + 1;
        for (int i = 0; i < dLen; i++) {
            if (_pdata[i] == findstr[0] && memcmp(_pdata+i, findstr, len) == 0) {
                return i;
            }
        }
        return -1;
    }

private:
    void writeString(const std::string &str) {
        uint32_t strLen = str.length();
        write(strLen);
        writeBytes(str.c_str(), strLen);
    }

    void readString(std::string &str) {
        uint32_t strLen;
        read(strLen);
        AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(_pdata, _pend, strLen);
        str.assign(getData(), strLen);
        _pdata += strLen;
    }

    void ensureFree(int len) {
        expand(len);
    }

    void readByte(char& b) {
        AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(_pdata, _pend, 1);
        b = *_pdata;
        _pdata++;
    }

    unsigned char* writeVarint32(uint32_t value, unsigned char *target);
    void readVarint32(uint32_t &value);
    void readVarint32FromArray(uint32_t &value) __attribute__((always_inline));
    void readVarint32Fallback(uint32_t &value);
    void readVarint32FallbackInline(uint32_t &value);
    void readVarint32Slow(uint32_t &value);

    unsigned char* writeVarint64(uint64_t value, unsigned char* target);
    void readVarint64(uint64_t &value);
    void readVarint64FromArray(uint64_t &value) __attribute__((always_inline));
    void readVarint64Fallback(uint64_t &value);
    void readVarint64FallbackInline(uint64_t &value);
    void readVarint64Slow(uint64_t &value);
private:
    inline void expand(int need) {
        if (_pend - _pfree < need) {
            int flen = (_pend - _pfree) + (_pdata - _pstart);
            int dlen = _pfree - _pdata;

            if (flen < need) {
                int bufsize = (_pend - _pstart) * 2;
                while (bufsize - dlen < need) {
                    bufsize <<= 1;
                }

                unsigned char *newbuf = allocateMemory(bufsize);
                assert(newbuf != NULL);
                if (dlen > 0) {
                    memcpy(newbuf, _pdata, dlen);
                }
                if (NULL == _pool) {
                    delete[] _pstart;
                }

                _pdata = _pstart = newbuf;
                _pfree = _pstart + dlen;
                _pend = _pstart + bufsize;
            } else {
                memmove(_pstart, _pdata, dlen);
                _pfree = _pstart + dlen;
                _pdata = _pstart;
            }
        }
    }

private:
    template<typename T>
    inline void _write(const T &value, SerializableType) {
        value.serialize(*this);
    }

    template<typename T, typename Alloc>
    inline void _read(T &value, SerializableType, Alloc *allocator) {
        value.deserialize(*this, allocator);
    }

    template<typename T>
    inline void _read(T &value, SerializableType) {
        value.deserialize(*this);
    }

    template<typename T>
    inline void _write(const T &value, SimpleType) {
        expand(sizeof(T));
        *reinterpret_cast<T *>(_pfree) = value;
        _pfree += sizeof(T);
    }

    template<typename T>
    inline void _read(T &value, SimpleType) {
        AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(_pdata, _pend, sizeof(T));
        value = *reinterpret_cast<const T *>(_pdata);
        _pdata += sizeof(T);
    }

    template<typename T>
    inline void _write(const std::vector<T> &value, SimpleType) {
        uint32_t size = value.size();
        write(size);
        writeBytes(&(value[0]), size * sizeof(T));
    }

    template<typename T>
    inline void _read(std::vector<T> &value, SimpleType) {
        uint32_t size = 0;
        read(size);
        value.resize(size);
        readBytes(&(value[0]), size * sizeof(T));
    }

    template<typename T>
    inline void _write(const std::vector<T> &value, SerializableType) {
        uint32_t size = value.size();
        write(size);
        for (uint32_t i = 0; i < size; ++i) {
            write(value[i]);
        }
    }

    template<typename T>
    inline void _read(std::vector<T> &value, SerializableType) {
        uint32_t size = 0;
        read(size);
        value.resize(size);
        for (uint32_t i = 0; i < size; ++i) {
            read(value[i]);
        }
    }

    template<typename T, typename Alloc>
    inline void _read(std::vector<T> &value, SerializableType,
                      Alloc *allocator)
    {
        uint32_t size = 0;
        read(size);
        value.resize(size);
        for (uint32_t i = 0; i < size; ++i) {
            read(value[i], allocator);
        }
    }

    void _writeConstString(const StringView &value);
    void _readConstString(StringView &value);
    void makeConstString(StringView &value, char *data, uint32_t len);

private:
    void dataBufferCorrupted() const;
private:
    unsigned char *_pstart;
    unsigned char *_pend;
    unsigned char *_pfree;
    unsigned char *_pdata;
    mem_pool::Pool *_pool;
    bool _needReleaseMemory;
private:
    std::map<std::string, DataBuffer *> _sectionMap;
private:
    friend class DataBufferTest;
};

template<>
inline void DataBuffer::write<std::string>(const std::string &value) {
    writeString(value);
}

template<>
inline void DataBuffer::read<std::string>(std::string &value) {
    readString(value);
}

template<>
inline void DataBuffer::write<StringView>(const StringView &value) {
    _writeConstString(value);
}

template<>
inline void DataBuffer::read<StringView>(StringView &value) {
    _readConstString(value);
}

template<typename MultiT>
inline void DataBuffer::read(MultiValueType<MultiT> &value) {
    read(value, _pool);
}

template<typename MultiT, typename Alloc>
inline void DataBuffer::read(MultiValueType<MultiT> &value, Alloc *allocator) {
    assert(allocator);
    uint32_t len = 0;
    read(len);
    void *buf = NULL;
    if(len > 0) {
        buf = allocator->allocate(len);
        readBytes(buf, len);
    }
    value.init(buf);
}

template<typename MultiT>
inline void DataBuffer::write(const MultiValueType<MultiT> &value) {
    uint32_t len = value.getDataSize() + value.getHeaderSize();
    write(len);
    if (len > 0) {
        char header[MultiValueFormatter::VALUE_COUNT_MAX_BYTES];
        size_t countLen = MultiValueFormatter::encodeCount(
                value.getCount(), header, MultiValueFormatter::VALUE_COUNT_MAX_BYTES);
        writeBytes(header, countLen);
        if (len > countLen) {
            const char* data = value.getData();
            assert(data != nullptr);
            writeBytes(data, len - countLen);
        }
    }
}

template<>
inline void DataBuffer::write(const MultiValueType<MultiValueType<char>> &value) {
    uint32_t len = value.length();
    write(len);
    if (len > 0) {
        assert(value.hasEncodedCount());
        uint32_t count = value.getCount();
        if (count == 0) {
            // see xxxx://invalid/issue/23705044
            uint16_t zero = 0;
            writeBytes(&zero, sizeof(zero));
        } else {
            char header[MultiValueFormatter::VALUE_COUNT_MAX_BYTES];
            size_t countLen = MultiValueFormatter::encodeCount(value.getCount(),
                    header, MultiValueFormatter::VALUE_COUNT_MAX_BYTES);
            writeBytes(header, countLen);
            if (len > countLen) {
                const char *data = value.getData();
                assert(data != nullptr);
                writeBytes(data, len - countLen);
            }
        }
    }
}

template<>
inline void DataBuffer::write<bool>(const std::vector<bool> &value) {
    std::vector<uint8_t> vec(value.begin(), value.end());
    write(vec);
}

template<>
inline void DataBuffer::read<bool>(std::vector<bool> &value) {
    std::vector<uint8_t> vec;
    read(vec);
    value.assign(vec.begin(), vec.end());
}

template<>
inline void DataBuffer::_write(const uint32_t &value, SimpleType) {
    expand(sizeof(uint32_t) + 1);
    _pfree = writeVarint32(value, _pfree);
}

template<>
inline void DataBuffer::_read(uint32_t &value, SimpleType) {
    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(_pdata, _pend, sizeof(*_pdata));
    if (!((*_pdata) & 0x80)) {
        value = *_pdata;
        _pdata++;
        return;
    }
    readVarint32(value);
}

inline unsigned char* DataBuffer::writeVarint32(
        uint32_t value, unsigned char *target)
{
    target[0] = static_cast<unsigned char>(value | 0x80);
    if (value >= (1 << 7)) {
        target[1] = static_cast<unsigned char>((value >>  7) | 0x80);
        if (value >= (1 << 14)) {
            target[2] = static_cast<unsigned char>((value >> 14) | 0x80);
            if (value >= (1 << 21)) {
                target[3] = static_cast<unsigned char>((value >> 21) | 0x80);
                if (value >= (1 << 28)) {
                    target[4] = static_cast<unsigned char>(value >> 28);
                    return target + 5;
                } else {
                    target[3] &= 0x7F;
                    return target + 4;
                }
            } else {
                target[2] &= 0x7F;
                return target + 3;
            }
        } else {
            target[1] &= 0x7F;
            return target + 2;
        }
    } else {
        target[0] &= 0x7F;
        return target + 1;
    }
}

inline void DataBuffer::readVarint32(uint32_t &value) {
    if (likely(_pdata <  _pend) && !(*_pdata & 0x80)) {
        value = *_pdata;
        _pdata++;
    } else {
        readVarint32FallbackInline(value);
    }
}

inline void DataBuffer::readVarint32FallbackInline(uint32_t &value) {
    if (_pend - _pdata > MAX_VARINT32_BYTES ||
        (_pend > _pdata && !(_pend[-1] & 0x80)))
    {
        readVarint32FromArray(value);
    } else {
        readVarint32Slow(value);
    }
}

inline void DataBuffer::readVarint32FromArray(uint32_t &value) {
    uint32_t b;
    uint32_t result;
    unsigned char* buffer = _pdata;

    b = *(buffer++); result  = (b & 0x7F)      ; if (!(b & 0x80)) goto done;
    b = *(buffer++); result |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;
    b = *(buffer++); result |= (b & 0x7F) << 14; if (!(b & 0x80)) goto done;
    b = *(buffer++); result |= (b & 0x7F) << 21; if (!(b & 0x80)) goto done;
    b = *(buffer++); result |=  b         << 28; if (!(b & 0x80)) goto done;

done:
    value = result;
    _pdata = buffer;
}

inline unsigned char* DataBuffer::writeVarint64(uint64_t value, unsigned char* target) {
  // Splitting into 32-bit pieces gives better performance on 32-bit
  // processors.
  uint32_t part0 = static_cast<uint32_t>(value      );
  uint32_t part1 = static_cast<uint32_t>(value >> 28);
  uint32_t part2 = static_cast<uint32_t>(value >> 56);

  int size;

  // Here we can't really optimize for small numbers, since the value is
  // split into three parts.  Cheking for numbers < 128, for instance,
  // would require three comparisons, since you'd have to make sure part1
  // and part2 are zero.  However, if the caller is using 64-bit integers,
  // it is likely that they expect the numbers to often be very large, so
  // we probably don't want to optimize for small numbers anyway.  Thus,
  // we end up with a hardcoded binary search tree...
  if (part2 == 0) {
    if (part1 == 0) {
      if (part0 < (1 << 14)) {
        if (part0 < (1 << 7)) {
          size = 1; goto size1;
        } else {
          size = 2; goto size2;
        }
      } else {
        if (part0 < (1 << 21)) {
          size = 3; goto size3;
        } else {
          size = 4; goto size4;
        }
      }
    } else {
      if (part1 < (1 << 14)) {
        if (part1 < (1 << 7)) {
          size = 5; goto size5;
        } else {
          size = 6; goto size6;
        }
      } else {
        if (part1 < (1 << 21)) {
          size = 7; goto size7;
        } else {
          size = 8; goto size8;
        }
      }
    }
  } else {
    if (part2 < (1 << 7)) {
      size = 9; goto size9;
    } else {
      size = 10; goto size10;
    }
  }

  size10: target[9] = static_cast<uint8_t>((part2 >>  7) | 0x80);
  size9 : target[8] = static_cast<uint8_t>((part2      ) | 0x80);
  size8 : target[7] = static_cast<uint8_t>((part1 >> 21) | 0x80);
  size7 : target[6] = static_cast<uint8_t>((part1 >> 14) | 0x80);
  size6 : target[5] = static_cast<uint8_t>((part1 >>  7) | 0x80);
  size5 : target[4] = static_cast<uint8_t>((part1      ) | 0x80);
  size4 : target[3] = static_cast<uint8_t>((part0 >> 21) | 0x80);
  size3 : target[2] = static_cast<uint8_t>((part0 >> 14) | 0x80);
  size2 : target[1] = static_cast<uint8_t>((part0 >>  7) | 0x80);
  size1 : target[0] = static_cast<uint8_t>((part0      ) | 0x80);

  target[size-1] &= 0x7F;
  return target + size;
}

inline void DataBuffer::readVarint64(uint64_t &value) {
    if (likely(_pdata < _pend) && !(*_pdata & 0x80)) {
        value = *_pdata++;
    } else {
        readVarint64FallbackInline(value);
    }
}

inline void DataBuffer::readVarint64FallbackInline(uint64_t &value) {
    if (_pend - _pdata >= MAX_VARINT64_BYTES ||
        (_pend > _pdata && !(_pend[-1] & 0x80)))
    {
        readVarint64FromArray(value);
    } else {
        readVarint64Slow(value);
    }
}

inline void DataBuffer::readVarint64FromArray(uint64_t &value) {
    unsigned char* buffer = _pdata;
    uint32_t part0 = 0, part1 = 0, part2 = 0;
    uint32_t b;
    b = *(buffer++); part0  = (b & 0x7F)      ; if (!(b & 0x80)) goto done;
    b = *(buffer++); part0 |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;
    b = *(buffer++); part0 |= (b & 0x7F) << 14; if (!(b & 0x80)) goto done;
    b = *(buffer++); part0 |= (b & 0x7F) << 21; if (!(b & 0x80)) goto done;
    b = *(buffer++); part1  = (b & 0x7F)      ; if (!(b & 0x80)) goto done;
    b = *(buffer++); part1 |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;
    b = *(buffer++); part1 |= (b & 0x7F) << 14; if (!(b & 0x80)) goto done;
    b = *(buffer++); part1 |= (b & 0x7F) << 21; if (!(b & 0x80)) goto done;
    b = *(buffer++); part2  = (b & 0x7F)      ; if (!(b & 0x80)) goto done;
    b = *(buffer++); part2 |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;

done:
    _pdata = buffer;
    value = (static_cast<uint64_t>(part0)      ) |
            (static_cast<uint64_t>(part1) << 28) |
            (static_cast<uint64_t>(part2) << 56);
}

template<>
inline void DataBuffer::_write(const int64_t &value, SimpleType) {
    expand(10);
    uint64_t t = (value << 1) ^ (value >> 63);
    _pfree = writeVarint64(t, _pfree);
}

template<>
inline void DataBuffer::_read(int64_t &value, SimpleType) {
    uint64_t t;
    readVarint64(t);
    value = (t >> 1) ^ - static_cast<int64_t>(t & 1);
}

}
