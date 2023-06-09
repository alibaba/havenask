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
#include "indexlib/index/common/data_structure/CompressOffsetReader.h"

#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/common/Constant.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, CompressOffsetReader);

CompressOffsetReader::CompressOffsetReader(bool disableGuardOffset)
    : _docCount(0)
    , _disableGuardOffset(disableGuardOffset)
    , _isSessionReader(false)
{
}

CompressOffsetReader::~CompressOffsetReader() {}

Status CompressOffsetReader::Init(uint32_t docCount,
                                  const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                                  const std::shared_ptr<indexlib::file_system::SliceFileReader>& expandSliceFile)
{
    uint8_t* buffer = (uint8_t*)fileReader->GetBaseAddress();
    auto fileStream = indexlib::file_system::FileStreamCreator::CreateFileStream(fileReader, nullptr);
    uint64_t bufferLen = fileStream->GetStreamLength();
    assert(bufferLen >= 4);
    assert(!_u32CompressReader);
    assert(!_u64CompressReader);
    size_t compressLen = bufferLen - sizeof(uint32_t); // minus tail len
    uint32_t magicTail = 0;
    auto [status, readed] =
        fileStream->Read(&magicTail, sizeof(magicTail), bufferLen - 4, indexlib::file_system::ReadOption())
            .StatusWith();
    RETURN_IF_STATUS_ERROR(status, "read data from file-stream failed.");

    if (readed != sizeof(magicTail)) {
        RETURN_STATUS_ERROR(IOError, "read magic tail failed, expected length [%lu], actual [%lu]", sizeof(magicTail),
                            readed);
    }
    uint32_t itemCount = 0;

    if (magicTail == UINT32_OFFSET_TAIL_MAGIC) {
        if (expandSliceFile) {
            RETURN_STATUS_ERROR(InternalError, "updatable compress offset should be u64 offset");
        }

        _u32CompressReader.reset(new indexlib::index::EquivalentCompressReader<uint32_t>());
        if (buffer) {
            _u32CompressReader->Init(buffer, compressLen, nullptr);
        } else {
            _u32CompressReader->Init(fileReader);
        }
        _u32CompressSessionReader =
            _u32CompressReader->CreateSessionReader(nullptr, indexlib::index::EquivalentCompressSessionOption());
        itemCount = _u32CompressReader->Size();
    } else if (magicTail == UINT64_OFFSET_TAIL_MAGIC) {
        std::shared_ptr<indexlib::util::BytesAlignedSliceArray> byteSliceArray;
        if (expandSliceFile) {
            byteSliceArray = expandSliceFile->GetBytesAlignedSliceArray();
        }

        _u64CompressReader.reset(new indexlib::index::EquivalentCompressReader<uint64_t>());
        if (buffer) {
            _u64CompressReader->Init(buffer, compressLen, byteSliceArray);
        } else {
            _u64CompressReader->Init(fileReader);
        }
        _u64CompressSessionReader =
            _u64CompressReader->CreateSessionReader(nullptr, indexlib::index::EquivalentCompressSessionOption());
        itemCount = _u64CompressReader->Size();
    } else {
        RETURN_STATUS_ERROR(InternalError, "Attribute compressed offset file magic tail not match");
    }

    if (GetOffsetCount(docCount) != itemCount) {
        RETURN_STATUS_ERROR(InternalError, "Attribute compressed offset item size do not match docCount");
    }
    _fileReader = fileReader;
    _sliceFileReader = expandSliceFile;
    _docCount = docCount;
    return Status::OK();
}

} // namespace indexlibv2::index
