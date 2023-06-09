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
#include "indexlib/index/kkv/common/ChunkWriter.h"

#include "indexlib/index/kkv/Constant.h"
namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, ChunkWriter);

Status ChunkWriter::Open(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                         const indexlib::file_system::WriterOption& writerOption, const std::string& fileName)
{
    auto [status, fileWriter] = directory->CreateFileWriter(fileName, writerOption).StatusWith();
    if (!status.IsOK()) {
        return status;
    }
    _fileWriter = fileWriter;
    return Status::OK();
}

Status ChunkWriter::Close()
{
    if (!FlushChunkBuffer()) {
        return Status::IOError("flush chunk buf failed");
    }
    assert(_fileWriter);
    return _fileWriter->Close().Status();
}

ChunkItemOffset ChunkWriter::InsertItem(autil::StringView data)
{
    assert(data.data() != nullptr && data.size() > 0UL);
    size_t inChunkOffset = _chunkBuf.size();
    if (inChunkOffset > ChunkItemOffset::MAX_IN_CHUNK_OFFSET) {
        if (!FlushChunkBuffer()) {
            return ChunkItemOffset::Invalid();
        }
        if (!PaddingChunk()) {
            return ChunkItemOffset::Invalid();
        }
        UpdateChunkOffset();
    }

    inChunkOffset = _chunkBuf.size();
    AppendData(data);
    return ChunkItemOffset::Of(_chunkOffset, inChunkOffset);
}

void ChunkWriter::AppendData(autil::StringView data)
{
    assert(data.size() > 0);
    _chunkBuf.insert(_chunkBuf.end(), data.data(), data.data() + data.size());
}

bool ChunkWriter::FlushChunkBuffer()
{
    if (_chunkBuf.empty()) {
        return true;
    }
    assert(_chunkBuf.size() <= MAX_CHUNK_DATA_LEN);

    ChunkMeta chunkMeta {static_cast<uint32_t>(_chunkBuf.size()), 0};
    assert(_fileWriter);
    if (!_fileWriter->Write(&chunkMeta, sizeof(ChunkMeta)).OK()) {
        AUTIL_LOG(ERROR, "write chunk meta failed");
        return false;
    }
    if (!_fileWriter->Write(_chunkBuf.data(), _chunkBuf.size()).OK()) {
        AUTIL_LOG(ERROR, "write chunk data failed");
        return false;
    }

    _chunkBuf.clear();
    return true;
}

bool ChunkWriter::PaddingChunk()
{
    if (_chunkOffsetAlignBit == 0) {
        return true;
    }

    size_t fileLen = GetLogicFileLength();
    size_t alignUnitSize = (size_t)1 << _chunkOffsetAlignBit;
    size_t alignUnitCount = (fileLen + alignUnitSize - 1) >> _chunkOffsetAlignBit;
    size_t alignFileLen = alignUnitCount << _chunkOffsetAlignBit;

    if (alignFileLen == fileLen) {
        return true;
    }
    std::vector<char> padding(alignFileLen - fileLen, 0);
    assert(_fileWriter);
    if (!_fileWriter->Write(padding.data(), padding.size()).OK()) {
        AUTIL_LOG(ERROR, "write padding failed, len[%lu]", padding.size());
        return false;
    }
    return true;
}

void ChunkWriter::UpdateChunkOffset()
{
    _chunkOffset = GetLogicFileLength();
    assert(((_chunkOffset >> _chunkOffsetAlignBit) << _chunkOffsetAlignBit) == _chunkOffset);
    assert(!ChunkItemOffset::IsOffsetOutOfRange(_chunkOffset, 0));
}

bool ChunkWriter::TEST_ForceFlush()
{
    if (!FlushChunkBuffer() || !PaddingChunk()) {
        return false;
    }
    UpdateChunkOffset();
    return true;
}

} // namespace indexlibv2::index