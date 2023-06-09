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
#include "indexlib/file_system/file/SwapMmapFileNode.h"

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/mman.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/MMapFile.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/MmapFileNode.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

namespace indexlib { namespace file_system {
class PackageOpenMeta;
}} // namespace indexlib::file_system

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, SwapMmapFileNode);

SwapMmapFileNode::SwapMmapFileNode(size_t fileSize, const BlockMemoryQuotaControllerPtr& memController) noexcept
    : MmapFileNode(LoadConfig(), memController, true)
    , _cursor(0)
    , _remainFile(true)
    , _readOnly(true)
{
    _length = fileSize;
}

SwapMmapFileNode::~SwapMmapFileNode() noexcept
{
    if (_cleanFunc) {
        _cleanFunc();
        _cleanFunc = nullptr;
    }
    if (_remainFile) {
        auto ec = Sync().Code();
        if (unlikely(ec != FSEC_OK)) {
            AUTIL_LOG(ERROR, "Sync [%s] failed, ec [%d]", GetPhysicalPath().c_str(), ec);
        }
    }
    auto ret = Close();
    if (unlikely(!ret.OK())) {
        AUTIL_LOG(ERROR, "Close failed, ec[%d]", ret.Code());
    }

    if (!_remainFile && !GetPhysicalPath().empty()) {
        auto ec = FslibWrapper::DeleteDir(GetPhysicalPath(), DeleteOption::NoFence(true)).Code();
        if (unlikely(ec != FSEC_OK)) {
            AUTIL_LOG(ERROR, "Delete file if exist failed: [%s], ec [%d]", GetPhysicalPath().c_str(), ec);
        }
    }
}

ErrorCode SwapMmapFileNode::WarmUp() noexcept
{
    RETURN_IF_FS_ERROR(LoadData(), "load data for file[%s] failed", DebugString().c_str());
    _populated = true;
    return FSEC_OK;
}

FSResult<void> SwapMmapFileNode::OpenForRead(const string& logicalPath, const string& physicalPath,
                                             FSOpenType openType) noexcept
{
    _logicalPath = logicalPath;
    _physicalPath = PathUtil::NormalizePath(physicalPath);
    _openType = openType;
    _readOnly = true;

    RETURN_IF_FS_ERROR(MmapFileNode::DoOpen(_physicalPath, openType, -1), "mmap open file [%s] failed",
                       _physicalPath.c_str());
    _cursor = _length;
    _remainFile = true;
    return FSEC_OK;
}

FSResult<void> SwapMmapFileNode::OpenForWrite(const string& logicalPath, const string& physicalPath,
                                              FSOpenType openType) noexcept
{
    _logicalPath = logicalPath;
    _physicalPath = PathUtil::NormalizePath(physicalPath);
    _openType = openType;
    _readOnly = false;

    int mapFlag = MAP_SHARED;
    int prot = PROT_READ | PROT_WRITE;
    _type = FSFT_MMAP;

    AUTIL_LOG(DEBUG,
              "MMap file[%s], loadStrategy[%s], openType[%d], "
              "type[%d], prot [%d], mapFlag [%d]",
              physicalPath.c_str(), ToJsonString(*_loadStrategy, true).c_str(), openType, _type, prot, mapFlag);
    auto ec = FslibWrapper::MkDirIfNotExist(PathUtil::GetParentDirPath(_physicalPath)).Code();
    RETURN_IF_FS_ERROR(ec, "make dir [%s] failed", PathUtil::GetParentDirPath(_physicalPath).c_str());
    auto [ec2, mmapFile] = FslibWrapper::MmapFile(_physicalPath, fslib::WRITE, NULL, _length, prot, mapFlag, 0, -1);
    RETURN_IF_FS_ERROR(ec2, "mmap file [%s] failed", _physicalPath.c_str());
    _file = std::move(mmapFile);
    _data = _file->getBaseAddress();
    _warmup = false;
    _remainFile = false;
    _populated = true;
    return FSEC_OK;
}

ErrorCode SwapMmapFileNode::DoOpen(const string& path, FSOpenType openType, int64_t fileLength) noexcept
{
    assert(false);
    return FSEC_NOTSUP;
}

ErrorCode SwapMmapFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept
{
    AUTIL_LOG(ERROR, "SwapMmapFileNode not support open in package file!");
    return FSEC_NOTSUP;
}

void SwapMmapFileNode::Resize(size_t size) noexcept
{
    if (size > _length) {
        AUTIL_LOG(ERROR, "not support resize to length[%lu] over file capacity[%lu]", size, _length);
        return;
    }
    _cursor = size;
}

FSResult<size_t> SwapMmapFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    if (offset + length > _cursor) {
        AUTIL_LOG(ERROR, "read file [%s] out of range, offset [%lu], read length: [%lu], file length: [%lu]",
                  DebugString().c_str(), offset, length, _cursor);
        return {FSEC_BADARGS, 0};
    }
    return MmapFileNode::Read(buffer, length, offset, option);
}

util::ByteSliceList* SwapMmapFileNode::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    if (offset + length > _cursor) {
        AUTIL_LOG(ERROR,
                  "read file [%s] out of range, "
                  "offset [%lu], read length: [%lu], file length: [%lu]",
                  DebugString().c_str(), offset, length, _cursor);
        assert(false);
        return nullptr;
    }
    return MmapFileNode::ReadToByteSliceList(length, offset, option);
}

FSResult<size_t> SwapMmapFileNode::Write(const void* buffer, size_t length) noexcept
{
    if (GetOpenType() != FSOT_MMAP) {
        return {FSEC_OK, 0};
    }

    assert(_file);
    size_t toWriteLen = length;
    if (_cursor + length > _length) {
        toWriteLen = _length > _cursor ? _length - _cursor : 0;
    }

    assert(_data);
    memcpy((char*)_data + _cursor, buffer, toWriteLen);
    _cursor += toWriteLen;
    return {FSEC_OK, toWriteLen};
}

FSResult<void> SwapMmapFileNode::Sync() noexcept
{
    if (GetOpenType() != FSOT_MMAP) {
        return FSEC_OK;
    }

    if (_file) {
        auto fslibEc = _file->flush();
        if (fslibEc != fslib::ErrorCode::EC_OK) {
            AUTIL_LOG(ERROR, "flush[%s] failed", DebugString().c_str());
            return ParseFromFslibEC(fslibEc);
        }
    }
    return FSEC_OK;
}
}} // namespace indexlib::file_system
