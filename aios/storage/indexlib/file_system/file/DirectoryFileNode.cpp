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
#include "indexlib/file_system/file/DirectoryFileNode.h"

#include <cstddef>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib { namespace file_system {
class PackageOpenMeta;
}} // namespace indexlib::file_system

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, DirectoryFileNode);

DirectoryFileNode::DirectoryFileNode() noexcept {}

DirectoryFileNode::~DirectoryFileNode() noexcept {}

ErrorCode DirectoryFileNode::DoOpen(const string& path, FSOpenType openType, int64_t fileLength) noexcept
{
    return FSEC_OK;
}

ErrorCode DirectoryFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept
{
    AUTIL_LOG(ERROR, "Not support DoOpen with PackageOpenMeta");
    return FSEC_NOTSUP;
}

FSFileType DirectoryFileNode::GetType() const noexcept { return FSFT_DIRECTORY; }

size_t DirectoryFileNode::GetLength() const noexcept { return 0; }

void* DirectoryFileNode::GetBaseAddress() const noexcept
{
    AUTIL_LOG(ERROR, "Not support GetBaseAddress");
    assert(false);
    return nullptr;
}

FSResult<size_t> DirectoryFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    AUTIL_LOG(ERROR, "Not support GetBaseAddress");
    assert(false);
    return {FSEC_NOTSUP, 0};
}

ByteSliceList* DirectoryFileNode::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    AUTIL_LOG(ERROR, "Not support GetBaseAddress");
    assert(false);
    return NULL;
}

FSResult<size_t> DirectoryFileNode::Write(const void* buffer, size_t length) noexcept
{
    AUTIL_LOG(ERROR, "Not support GetBaseAddress");
    assert(false);
    return {FSEC_NOTSUP, 0};
}

FSResult<void> DirectoryFileNode::Close() noexcept { return FSEC_OK; }
}} // namespace indexlib::file_system
