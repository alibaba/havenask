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
#include "indexlib/file_system/file/SliceFileNode.h"

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"
#include "indexlib/util/slice_array/BytesAlignedSliceArray.h"

namespace indexlib { namespace file_system {
class PackageOpenMeta;
}} // namespace indexlib::file_system

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, SliceFileNode);

SliceFileNode::SliceFileNode(uint64_t sliceLen, int32_t sliceNum,
                             const BlockMemoryQuotaControllerPtr& memController) noexcept
    : _bytesAlignedSliceArray(new BytesAlignedSliceArray(
          SimpleMemoryQuotaControllerPtr(new SimpleMemoryQuotaController(memController)), sliceLen, sliceNum))
    , _sliceNum(sliceNum)
{
}

SliceFileNode::~SliceFileNode() noexcept
{
    if (_cleanFunc) {
        _cleanFunc();
        _cleanFunc = nullptr;
    }
    [[maybe_unused]] auto ret = Close();
    assert(ret.OK());
}

ErrorCode SliceFileNode::DoOpen(const string& path, FSOpenType openType, int64_t fileLength) noexcept
{
    assert(openType == FSOT_SLICE);
    return FSEC_OK;
}

ErrorCode SliceFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept
{
    AUTIL_LOG(ERROR, "Not support DoOpen with PackageOpenMeta");
    return FSEC_NOTSUP;
}

FSFileType SliceFileNode::GetType() const noexcept { return FSFT_SLICE; }

size_t SliceFileNode::GetLength() const noexcept
{
    assert(_bytesAlignedSliceArray);
    return _bytesAlignedSliceArray->GetTotalUsedBytes();
}

void* SliceFileNode::GetBaseAddress() const noexcept
{
    assert(_bytesAlignedSliceArray);
    if (_sliceNum > 1) {
        AUTIL_LOG(ERROR, "Multi slice not support GetBaseAddress");
        return NULL;
    }
    return _bytesAlignedSliceArray->GetValueAddress(0);
}

FSResult<size_t> SliceFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    if (_sliceNum > 1) {
        AUTIL_LOG(ERROR, "Multi slice not support Read");
        return {FSEC_BADARGS, 0};
    }
    if (offset + length > GetSliceFileLength()) {
        return {FSEC_BADARGS, 0};
    }
    char* base = (char*)GetBaseAddress();

    memcpy(buffer, (void*)(base + offset), length);
    return {FSEC_OK, length};
}

ByteSliceList* SliceFileNode::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    AUTIL_LOG(ERROR, "Not support GetBaseAddress");
    return NULL;
}

FSResult<size_t> SliceFileNode::Write(const void* buffer, size_t length) noexcept
{
    assert(_bytesAlignedSliceArray);
    if (_sliceNum > 1) {
        AUTIL_LOG(ERROR, "Multi slice not support Write");
        return {FSEC_NOTSUP, 0};
    }

    RETURN2_IF_FS_EXCEPTION(_bytesAlignedSliceArray->Insert(buffer, length), 0, "Insert failed");
    return {FSEC_OK, length};
}

FSResult<void> SliceFileNode::Close() noexcept { return FSEC_OK; }
}} // namespace indexlib::file_system
