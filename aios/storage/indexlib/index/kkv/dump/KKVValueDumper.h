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

#include "autil/MultiValueFormatter.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/common/ChunkWriter.h"
#include "indexlib/index/kkv/common/NormalOnDiskSKeyNode.h"

namespace indexlibv2::index {

class KKVValueDumper
{
public:
    KKVValueDumper(int32_t valueFixedLen) : _valueFixedLen(valueFixedLen), _curMaxValueLen(0)
    {
        _curMaxValueLen = _valueFixedLen > 0 ? _valueFixedLen : 0ul;
    }
    virtual ~KKVValueDumper() = default;

public:
    Status Init(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                const indexlib::file_system::WriterOption& writerOption)
    {
        _chunkWriter = std::make_unique<ChunkWriter>(ON_DISK_VALUE_CHUNK_ALIGN_BIT);
        assert(_chunkWriter);
        return _chunkWriter->Open(directory, writerOption, KKV_VALUE_FILE_NAME);
    }

    std::pair<Status, ChunkItemOffset> Dump(autil::StringView value)
    {
        auto [status, offset] = _valueFixedLen > 0 ? DumpFixedLenValue(value) : DumpVarLenValue(value);
        if (status.IsOK() && offset.chunkOffset >= NAX_VALUE_CHUNK_OFFSET) {
            return {Status::OutOfRange("value chunk offset:[%lu] out of range:[%lu]", offset.chunkOffset,
                                       NAX_VALUE_CHUNK_OFFSET),
                    ChunkItemOffset::Invalid()};
        }
        return {status, offset};
    }

    Status Close()
    {
        assert(_chunkWriter);
        return _chunkWriter->Close();
    }

    size_t GetMaxValueLen() const { return _curMaxValueLen; }

private:
    std::pair<Status, ChunkItemOffset> DumpFixedLenValue(autil::StringView value)
    {
        assert(_chunkWriter);
        assert((size_t)_valueFixedLen == value.size());

        auto valueOffset = _chunkWriter->InsertItem(value);
        if (!valueOffset.IsValidOffset()) {
            return {Status::IOError("insert item failed, size[%lu]", value.size()), ChunkItemOffset::Invalid()};
        }

        return {Status::OK(), valueOffset};
    }

    std::pair<Status, ChunkItemOffset> DumpVarLenValue(autil::StringView value)
    {
        assert(_chunkWriter);

        char head[4];
        size_t headSize = autil::MultiValueFormatter::encodeCount(value.size(), head, 4);
        auto valueOffset = _chunkWriter->InsertItem({head, headSize});
        if (!valueOffset.IsValidOffset()) {
            return {Status::IOError("insert item failed, size[%lu]", headSize), ChunkItemOffset::Invalid()};
        }

        _chunkWriter->AppendData(value);
        _curMaxValueLen = std::max(_curMaxValueLen, headSize + value.size());

        return {Status::OK(), valueOffset};
    }

private:
    std::unique_ptr<indexlibv2::index::ChunkWriter> _chunkWriter;
    int32_t _valueFixedLen;
    size_t _curMaxValueLen;
};

} // namespace indexlibv2::index
