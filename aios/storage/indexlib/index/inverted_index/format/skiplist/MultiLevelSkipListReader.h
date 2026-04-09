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

#include <memory>

#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/format/skiplist/SkipListReader.h"

struct LengthInfo {
    uint8_t lengths[4]; // Each controlByte corresponds to the length of 4 fields.
};
constexpr LengthInfo MakeLengthInfo(uint8_t controlByte) {
    LengthInfo info{};
    for (uint8_t i = 0; i < 4; ++i) {
        info.lengths[i] = static_cast<uint8_t>(((controlByte >> (i * 2)) & 0x03) + 1);
    }
    return info;
}
constexpr std::array<LengthInfo, 256> MakeLengthTable() {
    std::array<LengthInfo, 256> table{};
    for (int c = 0; c < 256; ++c) {
        table[c] = MakeLengthInfo(static_cast<uint8_t>(c));
    }
    return table;
}
constexpr std::array<uint8_t, 256 * 3> MakeTotalLenTable() {
    std::array<uint8_t, 256 * 3> table{};
    for (int count = 2; count <= 4; ++count) {
        int idx = count - 2; // 2 -> 0, 3 -> 1, 4 -> 2
        for (int c = 0; c < 256; ++c) {
            uint8_t controlByte = static_cast<uint8_t>(c);
            uint8_t sum = 0;
            for (int i = 0; i < count; ++i) {
                uint8_t len = static_cast<uint8_t>(((controlByte >> (i * 2)) & 0x03) + 1);
                sum = static_cast<uint8_t>(sum + len);
            }
            table[idx * 256 + c] = sum;
        }
    }
    return table;
}

// Encoding 2 to 4 numbers can share the same decoding table, but the total length of different
// encodings is different.
constexpr auto LENGTH_TABLE = MakeLengthTable();
constexpr auto TOTAL_LEN_TABLE = MakeTotalLenTable();

namespace indexlib::index {

using namespace indexlib::file_system;

struct MultiLevelSkipListLevelMeta {
    uint32_t levelBegin;
    uint32_t levelEnd;
    uint32_t numSkipped;
    uint32_t skipDoc;
    uint32_t offset;
    uint32_t childPointer;
    uint32_t ttf;
};

class GroupVarintCompressor {
public:
    static constexpr uint32_t BUFFER_SIZE = 20;
    /**
     * @brief Compress 2~4 uint32 values into the specified buffer
     * @param values Input uint32 array
     * @param count Number of values [2,4]
     * @param output Output buffer pointer
     * @return Total number of bytes after compression
     */
    static size_t Compress(const uint32_t *values, uint8_t count, uint8_t *output) {
        assert(2 <= count && count <= 4);

        uint8_t *currentOutput = output;

        // Calculate the encoded length (in bytes) of each value and the control byte
        uint8_t lengths[4];
        uint8_t controlByte = 0;
        for (uint8_t i = 0; i < count; i++) {
            // 0 also occupies 1 byte
            lengths[i] =
                values[i] == 0 ? 1 : static_cast<uint8_t>((32 - __builtin_clz(values[i]) + 7) / 8);
            controlByte |= ((lengths[i] - 1) & 0x03) << (i * 2);
        }

        // Write control byte
        *currentOutput++ = controlByte;

        // Write the low bytes of each value according to its length
        for (uint8_t i = 0; i < count; i++) {
            *(uint32_t *)(currentOutput) = values[i];
            currentOutput += lengths[i];
        }

        return static_cast<size_t>(currentOutput - output);
    }

    /**
     * @brief Decompress leaf-level data into MultiLevelSkipListLevelMeta
     * @tparam EnableTTF Whether the TTF field is included
     */
    template <bool EnableTTF>
    static FSResult<void> DecompressLeaf(ByteSliceReader *reader,
                                         MultiLevelSkipListLevelMeta &meta) {
        uint8_t dataBuffer[BUFFER_SIZE];
        const uint8_t *data = nullptr;
        const LengthInfo *info = nullptr;
        uint8_t totalLen = 0;

        constexpr uint8_t COUNT = EnableTTF ? 3 : 2;

        auto ret = decodeCommon(reader, COUNT, info, dataBuffer, data, totalLen);
        RETURN_IF_FS_ERROR(ret.Code(), "Read DecompressLeaf failed");

        meta.skipDoc += Extract(data, info->lengths[0]);
        if constexpr (EnableTTF) {
            meta.ttf += Extract(data, info->lengths[1]);
            meta.offset += Extract(data, info->lengths[2]);
        } else {
            meta.offset += Extract(data, info->lengths[1]);
        }
        return FSEC_OK;
    }

    /**
     * @brief Decompress non-leaf-level data into MultiLevelSkipListLevelMeta
     * @tparam EnableTTF Whether the TTF field is included
     * @param levelBegin The start position of the next level, used for:
     *        childPointer = offset + levelBegin
     */
    template <bool EnableTTF>
    static FSResult<void> DecompressNonLeaf(ByteSliceReader *reader,
                                            MultiLevelSkipListLevelMeta &meta,
                                            uint32_t levelBegin) {
        uint8_t dataBuffer[BUFFER_SIZE];
        const uint8_t *data = nullptr;
        const LengthInfo *info = nullptr;
        uint8_t totalLen = 0;

        constexpr uint8_t COUNT = EnableTTF ? 4 : 3;

        auto ret = decodeCommon(reader, COUNT, info, dataBuffer, data, totalLen);
        RETURN_IF_FS_ERROR(ret.Code(), "Read DecompressNonLeaf failed");

        meta.skipDoc += Extract(data, info->lengths[0]);
        if constexpr (EnableTTF) {
            meta.ttf += Extract(data, info->lengths[1]);
            meta.offset += Extract(data, info->lengths[2]);
            meta.childPointer = Extract(data, info->lengths[3]) + levelBegin;
        } else {
            meta.offset += Extract(data, info->lengths[1]);
            meta.childPointer = Extract(data, info->lengths[2]) + levelBegin;
        }
        return FSEC_OK;
    }

    /**
     * @brief Only decompress the childPointer field of a non-leaf level
     * @tparam EnableTTF Whether the TTF field is included
     */
    template <bool EnableTTF>
    static FSResult<void> DecompressChildPointer(ByteSliceReader *reader,
                                                 MultiLevelSkipListLevelMeta &meta,
                                                 uint32_t levelBegin) {
        uint8_t dataBuffer[BUFFER_SIZE];
        const uint8_t *data = nullptr;
        const LengthInfo *info = nullptr;
        uint8_t totalLen = 0;

        constexpr uint8_t COUNT = EnableTTF ? 4 : 3;

        auto ret = decodeCommon(reader, COUNT, info, dataBuffer, data, totalLen);
        RETURN_IF_FS_ERROR(ret.Code(), "Read DecompressChildPointer failed");

        const uint8_t lastIdx = static_cast<uint8_t>(COUNT - 1);
        // Jump to the last field: totalLen - length of the last field
        data += totalLen - info->lengths[lastIdx];

        meta.childPointer = Extract(data, info->lengths[lastIdx]) + levelBegin;
        return FSEC_OK;
    }

    /**
     * @brief Common decoding interface for unit tests, decodes outlen values into output
     */
    static void Decode(ByteSliceReader *reader, uint32_t outlen, uint32_t *output) {
        assert(2 <= outlen && outlen <= 4);

        uint8_t dataBuffer[BUFFER_SIZE];
        const uint8_t *data = nullptr;
        const LengthInfo *info = nullptr;
        uint8_t totalLen = 0;
        uint8_t count = static_cast<uint8_t>(outlen);

        decodeCommon(reader, count, info, dataBuffer, data, totalLen).GetOrThrow();

        for (uint32_t i = 0; i < outlen; ++i) {
            output[i] = Extract(data, info->lengths[i]);
        }
    }

private:
    /**
     * @brief Common decode function: read control byte & read data with corresponding length
     *
     * @param reader      ByteSliceReader instance
     * @param count       Number of values to decode this time (2/3/4)
     * @param lengthInfo  Output: field length info LENGTH_TABLE[controlByte]
     * @param dataBuffer  Local buffer used for ReadMayCopy
     * @param data        Output: actual data start pointer (may point to internal buffer)
     * @param totalLen    Output: total number of bytes read this time
     */
    static inline FSResult<void> decodeCommon(ByteSliceReader *reader, uint8_t count,
                                              const LengthInfo *&lengthInfo, uint8_t *dataBuffer,
                                              const uint8_t *&data, uint8_t &totalLen) {
        assert(2 <= count && count <= 4);

        auto ret = reader->ReadByte();
        RETURN_IF_FS_ERROR(ret.Code(), "Read Gvint controlByte failed");
        uint8_t controlByte = ret.Value();

        // Unified length table: lengths[4]
        lengthInfo = &LENGTH_TABLE[controlByte];

        // totalLen comes from TOTAL_LEN_TABLE[(count-2) * 256 + controlByte]
        const uint8_t countIndex = static_cast<uint8_t>(count - 2);
        totalLen = TOTAL_LEN_TABLE[countIndex * 256 + controlByte];

        void *bufferPtr = dataBuffer;
        auto ret2 = reader->ReadMayCopy(bufferPtr, totalLen);
        RETURN_IF_FS_ERROR(ret2.Code(), "Read Gvint content failed");
        if (unlikely(ret2.Value() != totalLen)) {
            RETURN_IF_FS_ERROR(FSEC_ERROR, "Read Gvint content length mismatch");
        }
        data = static_cast<const uint8_t *>(bufferPtr);
        return FSEC_OK;
    }

    /**
     * @brief Decode a uint32 from buffer using len bytes and advance the data pointer
     */
    static inline uint32_t Extract(const uint8_t *&data, uint8_t len) {
        uint32_t v;
        switch (len) {
        case 1:
            v = data[0];
            break;
        case 2:
            v = *(const uint16_t *)(data);
            break;
        case 3:
            v = *(const uint16_t *)(data) | (uint32_t(data[2]) << 16);
            break;
        case 4:
        default:
            v = *(const uint32_t *)(data);
            break;
        }
        data += len;
        return v;
    }

private:
    static inline alog::Logger *_logger =
        alog::Logger::getLogger("indexlib.index.GroupVarintCompressor");
};

template <bool EnableTTF>
class MultiLevelSkipListReader : public SkipListReader {
private:
    using SkipListReader::Load;

public:
    using keytype_t = uint32_t;
    MultiLevelSkipListReader(bool isReferenceCompress = false);
    virtual ~MultiLevelSkipListReader();

public:
    void Load(autil::mem_pool::Pool *pool, const util::ByteSliceList *byteSliceList, uint32_t start,
              uint32_t end, const uint32_t &itemCount) override;

    void Load(autil::mem_pool::Pool *pool, util::ByteSlice *byteSlice, uint32_t start, uint32_t end,
              const uint32_t &itemCount) override;
    std::pair<Status, bool> SkipTo(uint32_t queryKey, uint32_t &key, uint32_t &prevKey,
                                   uint32_t &value, uint32_t &delta) override;
    uint32_t GetPrevKey() const override {
        return _lastDoc;
    }
    // After the pos list completes its skip list iterations, GetCurrentKey() will be called, which
    // needs to return _lastDoc.
    uint32_t GetCurrentKey() const override {
        if (unlikely(_readers == nullptr)) {
            return 0;
        }
        uint32_t doc = _metas[0].skipDoc;
        return doc == END_DOC ? _lastDoc : doc;
    }
    uint32_t GetCurrentTTF() const override {
        if (unlikely(_readers == nullptr)) {
            return 0;
        }
        return _metas[0].ttf;
    }
    uint32_t GetPrevTTF() const override {
        return _lastTTF;
    }
    // This function is used when the skip list has been traversed to find the last element in the
    // list; it should return _lastDoc.
    uint32_t GetLastKeyInBuffer() const override {
        return GetPrevKey();
    }
    uint32_t GetLastValueInBuffer() const override {
        if (unlikely(_readers == nullptr)) {
            return 0;
        }
        return _metas[0].offset;
    }

private:
    void allocateData(autil::mem_pool::Pool *pool);
    void LoadFirstItem();
    inline FSResult<void> SeekChild(uint8_t level);
    inline FSResult<void> LoadNextSkip(uint8_t level);
    template <typename Content>
    void LoadInternal(autil::mem_pool::Pool *pool, Content content, uint32_t start, uint32_t end,
                      uint32_t itemCount);
    const static uint32_t END_DOC = std::numeric_limits<uint32_t>::max();
    // This value does not change with the seek of the skip list
    uint8_t _skipLevel;
    file_system::ByteSliceReader *_readers;
    struct MultiLevelSkipListLevelMeta *_metas;
    uint8_t _numberOfSkipLevel;
    uint32_t _lastDoc;
    uint32_t _lastTTF;
    uint32_t _lastOffset;
    uint32_t _lastChildPointer;

    static constexpr uint64_t SkipInterval[MAX_SKIP_LEVEL] = {
        1ULL,                                                               // SKIP_MULTIPLIER^0 = 1
        SKIP_MULTIPLIER,                                                    // SKIP_MULTIPLIER^1
        SKIP_MULTIPLIER *SKIP_MULTIPLIER,                                   // SKIP_MULTIPLIER^2
        SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER,                  // SKIP_MULTIPLIER^3
        SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER, // SKIP_MULTIPLIER^4
        SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER
            *SKIP_MULTIPLIER, // SKIP_MULTIPLIER^5
        SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER
            *SKIP_MULTIPLIER, // SKIP_MULTIPLIER^6
        SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER
            *SKIP_MULTIPLIER *SKIP_MULTIPLIER, // SKIP_MULTIPLIER^7
        SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER
            *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER, // SKIP_MULTIPLIER^8
        SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER
            *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER *SKIP_MULTIPLIER // SKIP_MULTIPLIER^9
    };

    friend class MultiLevelSkipListWriter;

private:
    inline AUTIL_LOG_DECLARE();
};

template <bool EnableTTF>
MultiLevelSkipListReader<EnableTTF>::MultiLevelSkipListReader(bool isReferenceCompress)
    : _skipLevel(0),
      _readers(nullptr),
      _metas(nullptr),
      _numberOfSkipLevel(0),
      _lastDoc(0),
      _lastTTF(0),
      _lastOffset(0),
      _lastChildPointer(0) {}

template <bool EnableTTF>
MultiLevelSkipListReader<EnableTTF>::~MultiLevelSkipListReader() {}

template <bool EnableTTF>
void MultiLevelSkipListReader<EnableTTF>::LoadFirstItem() {
    _numberOfSkipLevel = _skipLevel;
    _lastDoc = 0;
    if constexpr (EnableTTF) {
        _lastTTF = 0;
    }
    _lastOffset = 0;
    _lastChildPointer = 0;
    _skippedItemCount = 0;
    // Load the first element of the skip list. If the skip list exists, at least one node must
    // exist.
    GroupVarintCompressor::DecompressLeaf<EnableTTF>(&_readers[0], _metas[0]).GetOrThrow();
    _metas[0].numSkipped = 1;
}

template <bool EnableTTF>
void MultiLevelSkipListReader<EnableTTF>::allocateData(autil::mem_pool::Pool *pool) {
    size_t readerSize = sizeof(file_system::ByteSliceReader) * _skipLevel;
    size_t metaSize = sizeof(MultiLevelSkipListLevelMeta) * _skipLevel;

    // Allocate aligned memory for each of the two types.
    char *readerData =
        static_cast<char *>(pool->allocateAlign(readerSize, alignof(file_system::ByteSliceReader)));
    char *metaData =
        static_cast<char *>(pool->allocateAlign(metaSize, alignof(MultiLevelSkipListLevelMeta)));

    // Initialize the ByteSliceMemoryReader array
    _readers = reinterpret_cast<file_system::ByteSliceReader *>(readerData);
    for (uint8_t i = 0; i < _skipLevel; ++i) {
        new (_readers + i) file_system::ByteSliceReader();
    }

    // Initialize the MultiLevelSkipListLevelMeta array
    memset(metaData, 0, metaSize);
    _metas = reinterpret_cast<MultiLevelSkipListLevelMeta *>(metaData);
}

template <bool EnableTTF>
void MultiLevelSkipListReader<EnableTTF>::Load(autil::mem_pool::Pool *pool,
                                               const util::ByteSliceList *byteSliceList,
                                               uint32_t start, uint32_t end,
                                               const uint32_t &itemCount) {
    SkipListReader::Load(byteSliceList, start, end);
    LoadInternal(pool, byteSliceList, start, end, itemCount);
}

template <bool EnableTTF>
void MultiLevelSkipListReader<EnableTTF>::Load(autil::mem_pool::Pool *pool,
                                               util::ByteSlice *byteSlice, uint32_t start,
                                               uint32_t end, const uint32_t &itemCount) {
    SkipListReader::Load(byteSlice, start, end);
    LoadInternal(pool, byteSlice, start, end, itemCount);
}

template <bool EnableTTF>
template <typename Content>
void MultiLevelSkipListReader<EnableTTF>::LoadInternal(autil::mem_pool::Pool *pool, Content content,
                                                       uint32_t start, uint32_t end,
                                                       uint32_t itemCount) {
    _skipLevel = 1 + std::log(itemCount) / std::log(SKIP_MULTIPLIER);
    _skipLevel = std::min(_skipLevel, MAX_SKIP_LEVEL);
    assert(_skipLevel != 0);
    allocateData(pool);

    // No encoding of level 0 length
    if (_skipLevel == 1) {
        _metas[0].levelBegin = start;
        _metas[0].levelEnd = end;
        if constexpr (std::is_const_v<std::remove_pointer_t<Content>>) {
            using NonConstType = std::remove_const_t<std::remove_pointer_t<Content>> *;
            _readers[0].Open(const_cast<NonConstType>(content)).GetOrThrow();
        } else {
            _readers[0].Open(content).GetOrThrow();
        }
        _readers[0].Seek(start).GetOrThrow();
    } else {
        uint32_t levelLen[MAX_SKIP_LEVEL];
        uint32_t totalLenExceptLevel0 = 0;
        levelLen[0] = 0;
        for (size_t i = 1; i < _skipLevel; i++) {
            levelLen[i] = _byteSliceReader.ReadVUInt32().GetOrThrow();
            totalLenExceptLevel0 += levelLen[i];
        }
        start = _byteSliceReader.Tell();
        // Calculate the length of level 0 based on the value of end.
        levelLen[0] = end - start - totalLenExceptLevel0;
        uint32_t baseOffset = start;
        for (size_t i = 0; i < _skipLevel; ++i) {
            _metas[i].levelBegin = baseOffset;
            baseOffset += levelLen[i];
            _metas[i].levelEnd = baseOffset;
            if constexpr (std::is_const_v<std::remove_pointer_t<Content>>) {
                using NonConstType = std::remove_const_t<std::remove_pointer_t<Content>> *;
                _readers[i].Open(const_cast<NonConstType>(content)).GetOrThrow();
            } else {
                _readers[i].Open(content).GetOrThrow();
            }
            _readers[i].Seek(_metas[i].levelBegin).GetOrThrow();
        }
    }
    LoadFirstItem();
}

template <bool EnableTTF>
std::pair<Status, bool> MultiLevelSkipListReader<EnableTTF>::SkipTo(
    uint32_t queryKey, uint32_t &key, uint32_t &prevKey, uint32_t &value, uint32_t &delta) {
    // _skipLevel:
    // 1. Offline index skiplist is always not empty
    // 2. Real-time index skiplist may be empty (because the real-time index skiplist snapshot may
    // have been forked when the skiplist was first created; in this case, simply check if the
    // skiplist is empty and return false).
    // _numberOfSkipLevel:
    // 1. If the skiplist is exhausted at this point, return false.
    if (unlikely(_skipLevel == 0 || _numberOfSkipLevel == 0)) {
        return std::make_pair(Status::OK(), false);
    }
    int32_t level = 0;

    // If there is no skip node, the process ends directly.
    if (queryKey <= _metas[0].skipDoc) {
        goto end;
    } else {
        // Pre-probe a level 0 node. If the node is found, return immediately without traversing
        // higher levels of the skip list.
        // If traversing a multi-level skip list and consecutive level 0 nodes are found, this will
        // be optimized to traversing a single-level skip list.
        auto ret = LoadNextSkip(0);
        RETURN2_IF_STATUS_ERROR(ret.Status(), false,
                                "MultiLevelSkipListReader prefetch level0 node failed");
        if (queryKey <= _metas[0].skipDoc) {
            goto end;
        }
    }

    while (level < _numberOfSkipLevel - 1 && queryKey > _metas[level + 1].skipDoc) {
        level++;
    }
    while (level >= 0) {
        if (queryKey > _metas[level].skipDoc) {
            auto ret = LoadNextSkip(level);
            RETURN2_IF_STATUS_ERROR(ret.Status(), false,
                                    "MultiLevelSkipListReader LoadNextSkip failed");
        } else {
            if (level > 0 && _lastChildPointer > _readers[level - 1].Tell()) {
                auto ret = SeekChild(level - 1);
                RETURN2_IF_STATUS_ERROR(ret.Status(), false,
                                        "MultiLevelSkipListReader SeekChild failed");
            }
            level--;
        }
    }
end:
    key = GetCurrentKey();
    prevKey = GetPrevKey();
    value = _lastOffset;
    delta = _metas[0].offset - _lastOffset;
    // The definition of _skippedItemCount is the number up to prev.
    _skippedItemCount = _metas[0].numSkipped - 1;
    return std::make_pair(Status::OK(), _metas[0].skipDoc != END_DOC);
}

template <bool EnableTTF>
FSResult<void> MultiLevelSkipListReader<EnableTTF>::LoadNextSkip(uint8_t level) {
    _lastDoc = _metas[level].skipDoc;
    if constexpr (EnableTTF) {
        _lastTTF = _metas[level].ttf;
    }
    _lastOffset = _metas[level].offset;
    _lastChildPointer = _metas[level].childPointer;
    _metas[level].numSkipped += SkipInterval[level];
    // Check if the skip list at this level has been traversed.
    // If skipping to data exceeding the skip list's range, the skip list is exhausted; check
    // BYTE_SLICE_EOF. Return false directly.
    if (_readers[level].Tell() == _metas[level].levelEnd ||
        _readers[level].Tell() == ByteSliceReader::BYTE_SLICE_EOF) {
        // Use END_DOC, not INVALID_DOCID, as -1 cannot represent document boundaries.
        _metas[level].skipDoc = END_DOC;
        if (_numberOfSkipLevel > level) _numberOfSkipLevel = level;
        return FSEC_OK;
    }
    // Read skip list data
    return level != 0
               ? GroupVarintCompressor::DecompressNonLeaf<EnableTTF>(
                     &_readers[level], _metas[level], _metas[level - 1].levelBegin)
               : GroupVarintCompressor::DecompressLeaf<EnableTTF>(&_readers[level], _metas[level]);
}

template <bool EnableTTF>
FSResult<void> MultiLevelSkipListReader<EnableTTF>::SeekChild(uint8_t level) {
    auto ret = _readers[level].Seek(_lastChildPointer);
    RETURN_IF_FS_ERROR(ret.Code(), "Seek Skiplist ChildPointer failed");
    _metas[level].numSkipped = _metas[level + 1].numSkipped - SkipInterval[level + 1];
    _metas[level].skipDoc = _lastDoc;
    if constexpr (EnableTTF) {
        _metas[level].ttf = _lastTTF;
    }
    _metas[level].offset = _lastOffset;
    if (level > 0) {
        return GroupVarintCompressor::DecompressChildPointer<EnableTTF>(
                           &_readers[level], _metas[level], _metas[level - 1].levelBegin);
    }
    return FSEC_OK;
}

typedef MultiLevelSkipListReader<false> MultiLevelPairValueSkipListReader;
typedef MultiLevelSkipListReader<true> MultiLevelTriValueSkipListReader;
} // namespace indexlib::index
