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
#ifndef __INDEXLIB_KKV_VALUE_WRITER_H
#define __INDEXLIB_KKV_VALUE_WRITER_H

#include <memory>

#include "autil/ConstString.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/SwapMmapFileWriter.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/MmapPool.h"

namespace indexlib { namespace index {

class KKVValueWriter
{
public:
    KKVValueWriter(bool useSwapFile = false);
    ~KKVValueWriter();

public:
    void Init(const file_system::DirectoryPtr& directory, size_t sliceSize, size_t sliceCount);

    char* GetValue(size_t offset)
    {
        if (offset >= mUsedBytes) {
            return NULL;
        }
        size_t sliceIdx = offset >> mPowerNum;
        size_t inSliceIdx = offset & mMask;
        return mBaseAddrVec[sliceIdx] + inSliceIdx;
    }

    const char* GetValue(size_t offset) const
    {
        if (offset >= mUsedBytes) {
            return NULL;
        }
        size_t sliceIdx = offset >> mPowerNum;
        size_t inSliceIdx = offset & mMask;
        return mBaseAddrVec[sliceIdx] + inSliceIdx;
    }

    size_t GetReserveBytes() const { return mReserveBytes; }
    size_t GetUsedBytes() const { return mUsedBytes; }

    size_t Append(const autil::StringView& value);

    void WarmUp();

private:
    size_t CalculatePowerNumber(size_t sliceSize);
    void GuaranteeSpace(size_t valueLen);

private:
    size_t mPowerNum;
    size_t mMask;
    size_t mSliceCount;
    std::vector<char*> mBaseAddrVec;

    size_t mReserveBytes;
    size_t mUsedBytes;
    util::MmapPool* mPool;
    file_system::DirectoryPtr mDir;
    std::vector<file_system::SwapMmapFileWriterPtr> mSwapFileWriterVec;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVValueWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_VALUE_WRITER_H
