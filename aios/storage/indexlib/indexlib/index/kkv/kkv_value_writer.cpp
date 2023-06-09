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
#include "indexlib/index/kkv/kkv_value_writer.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/file/SwapMmapFileWriter.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVValueWriter);

KKVValueWriter::KKVValueWriter(bool useSwapFile)
    : mPowerNum(0)
    , mMask(0)
    , mSliceCount(0)
    , mReserveBytes(0)
    , mUsedBytes(0)
    , mPool(NULL)
{
    if (!useSwapFile) {
        mPool = new util::MmapPool();
    }
}

KKVValueWriter::~KKVValueWriter()
{
    if (mPool) {
        for (size_t i = 0; i < mBaseAddrVec.size(); i++) {
            mPool->deallocate(mBaseAddrVec[i], (1 << mPowerNum));
        }
        DELETE_AND_SET_NULL(mPool);
    }
}

void KKVValueWriter::Init(const DirectoryPtr& directory, size_t sliceSize, size_t sliceCount)
{
    assert(sliceCount >= 1);
    mDir = directory;
    mPowerNum = CalculatePowerNumber(sliceSize);
    mSliceCount = sliceCount;
    mMask = ((size_t)1 << mPowerNum) - 1;
    mBaseAddrVec.reserve(sliceCount);
    mReserveBytes = (sliceCount == 1) ? sliceSize : ((size_t)1 << mPowerNum) * mSliceCount;
    if (!mPool) {
        mSwapFileWriterVec.reserve(sliceCount);
    }
}

size_t KKVValueWriter::Append(const StringView& value)
{
    GuaranteeSpace(value.length());

    size_t sliceIdx = mUsedBytes >> mPowerNum;
    size_t inSliceIdx = mUsedBytes & mMask;
    char* addr = mBaseAddrVec[sliceIdx] + inSliceIdx;
    memcpy(addr, value.data(), value.size());
    size_t ret = mUsedBytes;
    MEMORY_BARRIER();
    mUsedBytes += value.size();
    return ret;
}

size_t KKVValueWriter::CalculatePowerNumber(size_t sliceSize)
{
    size_t powerNum = 0;
    size_t alignSliceSize = 1 << powerNum;
    while (alignSliceSize < sliceSize) {
        ++powerNum;
        alignSliceSize = (1 << powerNum);
    }
    return powerNum;
}

void KKVValueWriter::GuaranteeSpace(size_t valueLen)
{
    size_t sliceSize = (size_t)(1) << mPowerNum;
    if (valueLen > sliceSize) {
        INDEXLIB_FATAL_ERROR(UnSupported, "valueLen [%lu] over sliceSize [%lu]", valueLen, sliceSize);
    }

    size_t currentSize = sliceSize * mBaseAddrVec.size();
    if (currentSize > mReserveBytes) {
        currentSize = mReserveBytes;
    }

    size_t remainSize = currentSize - mUsedBytes;
    if (remainSize >= valueLen) {
        return;
    }

    if (remainSize < valueLen) {
        // add padding
        mUsedBytes += remainSize;
    }

    if (mUsedBytes >= mReserveBytes) {
        INDEXLIB_FATAL_ERROR(OutOfMemory, "value writer exceed mem reserve, reserve size [%lu], used bytes [%lu]",
                             (size_t)mReserveBytes, (size_t)mUsedBytes);
    }

    if (mPool) {
        char* newBaseAddr = static_cast<char*>(mPool->allocate(sliceSize));
        mBaseAddrVec.push_back(newBaseAddr);
    } else {
        assert(mDir);
        string fileName = string(SWAP_MMAP_FILE_NAME) + "." + StringUtil::toString(mBaseAddrVec.size());
        FileWriterPtr fileWriter = mDir->CreateFileWriter(fileName, WriterOption::SwapMmap((int64_t)sliceSize));
        SwapMmapFileWriterPtr swapFileWriter = DYNAMIC_POINTER_CAST(SwapMmapFileWriter, fileWriter);
        swapFileWriter->Truncate(sliceSize).GetOrThrow();
        mBaseAddrVec.push_back((char*)swapFileWriter->GetBaseAddress());
        mSwapFileWriterVec.push_back(swapFileWriter);
    }
}

void KKVValueWriter::WarmUp()
{
    for (size_t i = 0; i < mSwapFileWriterVec.size(); i++) {
        if (mSwapFileWriterVec[i]) {
            THROW_IF_FS_ERROR(mSwapFileWriterVec[i]->WarmUp(), "warmup failed");
        }
        usleep(500000); // 0.5s
    }
}
}} // namespace indexlib::index
