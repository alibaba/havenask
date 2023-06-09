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
#include "indexlib/index/data_structure/uncompress_offset_reader.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, UncompressOffsetReader);

UncompressOffsetReader::UncompressOffsetReader(uint64_t offsetExtendThreshold, bool disableGuardOffset)
    : mU64Buffer(NULL)
    , mU32Buffer(NULL)
    , mDocCount(0)
    , mIsU32Offset(false)
    , mDisableGuardOffset(disableGuardOffset)
    , mOffsetThreshold(offsetExtendThreshold)
{
}

UncompressOffsetReader::~UncompressOffsetReader() {}

void UncompressOffsetReader::Init(uint32_t docCount, const FileReaderPtr& fileReader,
                                  const SliceFileReaderPtr& u64SliceFileReader)
{
    size_t fileLen = (u64SliceFileReader != nullptr) ? u64SliceFileReader->GetLength() : 0;
    if (fileLen > 0) {
        size_t u64OffsetFileLen = GetOffsetCount(docCount) * sizeof(uint64_t);
        if (fileLen != u64OffsetFileLen) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                                 "Attribute offset slice file length [%lu]"
                                 " is not right, only can be 0 or %lu",
                                 fileLen, u64OffsetFileLen);
        }
        // already extend to u64
        mFileReader = u64SliceFileReader;
        mSliceFileReader.reset();
    } else {
        assert(fileReader);
        mFileReader = fileReader;
        mSliceFileReader = u64SliceFileReader;
    }
    if (mFileReader->GetBaseAddress()) {
        InitFromBuffer((uint8_t*)mFileReader->GetBaseAddress(), mFileReader->GetLength(), docCount);
    } else {
        InitFromFile(mFileReader, docCount);
    }
}

void UncompressOffsetReader::InitFromBuffer(uint8_t* buffer, uint64_t bufferLen, uint32_t docCount)
{
    uint64_t expectOffsetCount = GetOffsetCount(docCount);
    if (bufferLen == expectOffsetCount * sizeof(uint32_t)) {
        mU32Buffer = (uint32_t*)buffer;
        mIsU32Offset = true;
    } else if (bufferLen == expectOffsetCount * sizeof(uint64_t)) {
        mU64Buffer = (uint64_t*)buffer;
        mIsU32Offset = false;
    } else {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Attribute offset file length is not consistent with segment info, "
                             "offset length is %ld != %ld or %ld, doc number in segment info is %d",
                             bufferLen, expectOffsetCount * sizeof(uint32_t), expectOffsetCount * sizeof(uint64_t),
                             (int32_t)docCount);
    }
    mDocCount = docCount;
}

void UncompressOffsetReader::InitFromFile(const file_system::FileReaderPtr& fileReader, uint32_t docCount)
{
    uint64_t expectOffsetCount = GetOffsetCount(docCount);
    mFileStream = file_system::FileStreamCreator::CreateConcurrencyFileStream(mFileReader, nullptr);
    auto bufferLen = mFileStream->GetStreamLength();
    if (bufferLen == expectOffsetCount * sizeof(uint32_t)) {
        mIsU32Offset = true;
    } else if (bufferLen == expectOffsetCount * sizeof(uint64_t)) {
        mIsU32Offset = false;
    } else {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Attribute offset file length is not consistent "
                             "with segment info, offset length is %ld, "
                             "doc number in segment info is %d",
                             bufferLen, (int32_t)docCount);
    }
    mDocCount = docCount;
}

void UncompressOffsetReader::ExtendU32OffsetToU64Offset()
{
    if (mU64Buffer) {
        return;
    }

    if (!mSliceFileReader) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "no slice file reader for extend offset usage.");
    }

    BytesAlignedSliceArrayPtr sliceArray = mSliceFileReader->GetBytesAlignedSliceArray();
    uint32_t bufferItemCount = 64 * 1024;
    // add extend buffer to decrease call Insert interface
    vector<uint64_t> buffer;
    buffer.resize(bufferItemCount);
    uint64_t* extendOffsetBuffer = (uint64_t*)buffer.data();

    uint32_t remainCount = GetOffsetCount(mDocCount);
    uint32_t* u32Begin = mU32Buffer;
    while (remainCount > 0) {
        uint32_t convertNum = bufferItemCount <= remainCount ? bufferItemCount : remainCount;
        ExtendU32OffsetToU64Offset(u32Begin, extendOffsetBuffer, convertNum);
        sliceArray->Insert(extendOffsetBuffer, convertNum * sizeof(uint64_t));
        remainCount -= convertNum;
        u32Begin += convertNum;
    }
    mU64Buffer = (uint64_t*)mSliceFileReader->GetBaseAddress();
    mIsU32Offset = false;
}

void UncompressOffsetReader::ExtendU32OffsetToU64Offset(uint32_t* u32Offset, uint64_t* u64Offset, uint32_t count)
{
    const uint32_t pipeLineNum = 8;
    uint32_t processMod = count % pipeLineNum;
    uint32_t processSize = count - processMod;
    uint32_t pos = 0;
    for (; pos < processSize; pos += pipeLineNum) {
        u64Offset[pos] = u32Offset[pos];
        u64Offset[pos + 1] = u32Offset[pos + 1];
        u64Offset[pos + 2] = u32Offset[pos + 2];
        u64Offset[pos + 3] = u32Offset[pos + 3];
        u64Offset[pos + 4] = u32Offset[pos + 4];
        u64Offset[pos + 5] = u32Offset[pos + 5];
        u64Offset[pos + 6] = u32Offset[pos + 6];
        u64Offset[pos + 7] = u32Offset[pos + 7];
    }
    for (; pos < count; ++pos) {
        u64Offset[pos] = u32Offset[pos];
    }
}
}} // namespace indexlib::index
