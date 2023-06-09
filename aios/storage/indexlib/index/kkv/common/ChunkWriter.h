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

#include "autil/StringView.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/kkv/common/ChunkDefine.h"

namespace indexlibv2::index {

class ChunkWriter
{
public:
    ChunkWriter(uint32_t chunkOffsetAlignBit) : _chunkOffsetAlignBit(chunkOffsetAlignBit) {}
    ~ChunkWriter() = default;

public:
    Status Open(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                const indexlib::file_system::WriterOption& writerOption, const std::string& fileName);
    Status Close();
    size_t GetLogicFileLength() const { return _fileWriter->GetLogicLength(); }
    bool TEST_ForceFlush();

public:
    ChunkItemOffset InsertItem(autil::StringView data);
    void AppendData(autil::StringView data);

protected:
    bool FlushChunkBuffer();
    bool PaddingChunk();
    void UpdateChunkOffset();

protected:
    std::vector<char> _chunkBuf;
    size_t _chunkOffset = 0;
    size_t _chunkOffsetAlignBit = 0;
    indexlib::file_system::FileWriterPtr _fileWriter;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
