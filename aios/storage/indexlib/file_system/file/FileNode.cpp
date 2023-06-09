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
#include "indexlib/file_system/file/FileNode.h"

#include <cstddef>
#include <stdint.h>

#include "future_lite/Common.h"
#include "future_lite/Try.h"
#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/package/PackageOpenMeta.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/metrics/MetricReporter.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileNode);

FileNode::FileNode() noexcept
    : _openType(FSOT_UNKNOWN)
    , _originalOpenType(FSOT_UNKNOWN)
    , _metricGroup(FSMG_LOCAL)
    , _dirty(false)
    , _inPackage(false)
{
}

FileNode::~FileNode() noexcept {}

FileNode::FileNode(const FileNode& other) noexcept
    : _logicalPath(other._logicalPath)
    , _physicalPath(other._physicalPath)
    , _openType(other._openType)
    , _originalOpenType(other._originalOpenType)
    , _metricGroup(other._metricGroup)
    , _dirty(other._dirty)
    , _inPackage(other._inPackage)
    , _fileLenReporter(other._fileLenReporter)
    , _fileNodeUseCountReporter(other._fileNodeUseCountReporter)
    , _fileNodeUseCountReporterPtr(_fileNodeUseCountReporter.get())
{
}

FSResult<void> FileNode::Open(const std::string& logicalPath, const std::string& physicalPath, FSOpenType openType,
                              int64_t fileLength) noexcept
{
    _logicalPath = logicalPath;
    _physicalPath = physicalPath;
    _openType = openType;
    RETURN_IF_FS_ERROR(DoOpen(_physicalPath, _openType, fileLength), "DoOpen file[%s] failed", DebugString().c_str());
    return FSEC_OK;
}

FSResult<void> FileNode::Open(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept
{
    _logicalPath = packageOpenMeta.GetLogicalFilePath();
    _physicalPath = packageOpenMeta.GetPhysicalFilePath();
    _openType = openType;
    _inPackage = true;
    RETURN_IF_FS_ERROR(DoOpen(packageOpenMeta, openType), "DoOpen file[%s] failed", DebugString().c_str());
    return FSEC_OK;
}

FSResult<size_t> FileNode::Prefetch(size_t length, size_t offset, ReadOption option) noexcept { return {FSEC_OK, 0}; }

FileNode* FileNode::Clone() const noexcept
{
    assert(false);
    AUTIL_LOG(ERROR, "FileNode [openType:%d] not support Clone!", _openType);
    return nullptr;
}

future_lite::Future<uint32_t> FileNode::ReadVUInt32Async(size_t offset, ReadOption option) noexcept(false)
{
    auto fileLen = GetLength();
    if (unlikely(offset >= fileLen)) {
        INDEXLIB_FATAL_ERROR(OutOfRange,
                             "read file out of range, offset: [%lu], "
                             "file length: [%lu]",
                             offset, fileLen);
    }
    auto bufferPtr = std::make_unique<uint64_t>(0);
    auto buffer = static_cast<void*>(bufferPtr.get());
    size_t bufferSize = std::min(sizeof(uint64_t), fileLen - offset);
    return ReadAsync(buffer, bufferSize, offset, option).thenValue([ptr = std::move(bufferPtr)](size_t readSize) {
        uint8_t* byte = reinterpret_cast<uint8_t*>(ptr.get());
        uint32_t value = (*byte) & 0x7f;
        int shift = 7;
        while ((*byte) & 0x80) {
            ++byte;
            value |= ((*byte & 0x7F) << shift);
            shift += 7;
        }
        return value;
    });
}

string FileNode::DebugString() const noexcept { return GetLogicalPath() + " -> " + GetPhysicalPath(); }

void FileNode::UpdateFileLengthMetric(FileSystemMetricsReporter* reporter) noexcept
{
    if (!reporter || !reporter->SupportReportFileLength()) {
        return;
    }

    if (_fileLenReporter == nullptr) {
        map<string, string> tagMap;
        reporter->ExtractTagInfoFromPath(GetLogicalPath(), tagMap);
        tagMap["fsType"] = string(GetFileNodeTypeStr(GetType()));
        _fileLenReporter = reporter->DeclareFileLengthMetricReporter(tagMap);
        _fileNodeUseCountReporter = reporter->DeclareFileNodeUseCountMetricReporter(tagMap);
        _fileNodeUseCountReporterPtr = _fileNodeUseCountReporter.get();
    }

    if (_fileLenReporter != nullptr) {
        _fileLenReporter->Record(GetLength());
    }
}

void FileNode::UpdateFileNodeUseCount(uint32_t count) noexcept
{
    // use pure pointer for thread safe
    if (_fileNodeUseCountReporterPtr) {
        _fileNodeUseCountReporterPtr->Record(count);
    }
}

}} // namespace indexlib::file_system
