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

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/file_system/ByteSliceWriter.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/BufferedByteSlice.h"
#include "indexlib/index/inverted_index/format/InMemPositionListDecoder.h"
#include "indexlib/index/inverted_index/format/PositionListFormat.h"
#include "indexlib/index/inverted_index/format/PositionListFormatOption.h"
#include "indexlib/index/inverted_index/format/skiplist/BufferedSkipListWriter.h"

namespace indexlib::index {

class PositionListEncoder
{
public:
    PositionListEncoder(
        const PositionListFormatOption& posListFormatOption, autil::mem_pool::Pool* byteSlicePool,
        autil::mem_pool::RecyclePool* bufferPool, const PositionListFormat* posListFormat = nullptr,
        format_versionid_t formatVersion = indexlibv2::config::InvertedIndexConfig::BINARY_FORMAT_VERSION)
        : _posListBuffer(byteSlicePool, bufferPool)
        , _lastPosInCurDoc(0)
        , _totalPosCount(0)
        , _posListFormatOption(posListFormatOption)
        , _isOwnFormat(false)
        , _posSkipListWriter(nullptr)
        , _posListFormat(posListFormat)
        , _byteSlicePool(byteSlicePool)
    {
        assert(posListFormatOption.HasPositionList());
        if (!posListFormat) {
            _posListFormat = new PositionListFormat(posListFormatOption, formatVersion);
            _isOwnFormat = true;
        }
        _posListBuffer.Init(_posListFormat);
    }

    ~PositionListEncoder()
    {
        if (_posSkipListWriter) {
            _posSkipListWriter->~BufferedSkipListWriter();
            _posSkipListWriter = nullptr;
        }
        if (_isOwnFormat) {
            DELETE_AND_SET_NULL(_posListFormat);
        }
    }

    void AddPosition(pos_t pos, pospayload_t posPayload);
    void EndDocument();
    void Flush();
    void Dump(const std::shared_ptr<file_system::FileWriter>& file);
    uint32_t GetDumpLength() const;

    InMemPositionListDecoder* GetInMemPositionListDecoder(autil::mem_pool::Pool* sessionPool) const;

    // public for test
    const util::ByteSliceList* GetPositionList() const { return _posListBuffer.GetByteSliceList(); }

    // only for ut
    void SetDocSkipListWriter(BufferedSkipListWriter* writer) { _posSkipListWriter = writer; }

    const PositionListFormat* GetPositionListFormat() const { return _posListFormat; }

private:
    void CreatePosSkipListWriter();
    void AddPosSkipListItem(uint32_t totalPosCount, uint32_t compressedPosSize, bool needFlush);
    void FlushPositionBuffer(uint8_t compressMode);

private:
    BufferedByteSlice _posListBuffer;
    pos_t _lastPosInCurDoc;                        // 4byte
    uint32_t _totalPosCount;                       // 4byte
    PositionListFormatOption _posListFormatOption; // 1byte
    bool _isOwnFormat;                             // 1byte
    BufferedSkipListWriter* _posSkipListWriter;
    const PositionListFormat* _posListFormat;
    autil::mem_pool::Pool* _byteSlicePool;

    friend class PositionListEncoderTest;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
