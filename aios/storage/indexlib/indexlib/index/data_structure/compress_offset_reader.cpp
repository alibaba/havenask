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
#include "indexlib/index/data_structure/compress_offset_reader.h"

#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, CompressOffsetReader);

CompressOffsetReader::CompressOffsetReader(bool disableGuardOffset)
    : mDocCount(0)
    , mDisableGuardOffset(disableGuardOffset)
    , mIsSessionReader(false)
{
}

CompressOffsetReader::~CompressOffsetReader() {}

void CompressOffsetReader::Init(uint32_t docCount, const FileReaderPtr& fileReader,
                                const SliceFileReaderPtr& expandSliceFile)
{
    uint8_t* buffer = (uint8_t*)fileReader->GetBaseAddress();
    auto fileStream = file_system::FileStreamCreator::CreateFileStream(fileReader, nullptr);
    uint64_t bufferLen = fileStream->GetStreamLength();
    assert(bufferLen >= 4);
    assert(!mU32CompressReader);
    assert(!mU64CompressReader);
    size_t compressLen = bufferLen - sizeof(uint32_t); // minus tail len
    uint32_t magicTail = 0;
    size_t readed =
        fileStream->Read(&magicTail, sizeof(magicTail), bufferLen - 4, file_system::ReadOption()).GetOrThrow();
    if (readed != sizeof(magicTail)) {
        INDEXLIB_FATAL_ERROR(FileIO, "read magic tail failed, expected length [%lu], actual [%lu]", sizeof(magicTail),
                             readed);
    }
    uint32_t itemCount = 0;

    if (magicTail == UINT32_OFFSET_TAIL_MAGIC) {
        if (expandSliceFile) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "updatable compress offset should be u64 offset");
        }

        mU32CompressReader.reset(new EquivalentCompressReader<uint32_t>());
        if (buffer) {
            mU32CompressReader->Init(buffer, compressLen, util::BytesAlignedSliceArrayPtr());
        } else {
            mU32CompressReader->Init(fileReader);
        }
        mU32CompressSessionReader = mU32CompressReader->CreateSessionReader(nullptr, EquivalentCompressSessionOption());
        itemCount = mU32CompressReader->Size();
    } else if (magicTail == UINT64_OFFSET_TAIL_MAGIC) {
        BytesAlignedSliceArrayPtr byteSliceArray;
        if (expandSliceFile) {
            byteSliceArray = expandSliceFile->GetBytesAlignedSliceArray();
        }

        mU64CompressReader.reset(new EquivalentCompressReader<uint64_t>());
        if (buffer) {
            mU64CompressReader->Init(buffer, compressLen, byteSliceArray);
        } else {
            mU64CompressReader->Init(fileReader);
        }
        mU64CompressSessionReader = mU64CompressReader->CreateSessionReader(nullptr, EquivalentCompressSessionOption());
        itemCount = mU64CompressReader->Size();
    } else {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Attribute compressed offset file magic tail not match");
    }

    if (GetOffsetCount(docCount) != itemCount) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Attribute compressed offset item size do not match docCount");
    }
    mFileReader = fileReader;
    mSliceFileReader = expandSliceFile;
    mDocCount = docCount;
}
}} // namespace indexlib::index
