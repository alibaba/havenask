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
#include "indexlib/file_system/file/FileReader.h"

#include <iosfwd>
#include <memory>

#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileReader);

FileReader::FileReader() noexcept : _offset(0) {}

FileReader::~FileReader() noexcept {}

FSResult<size_t> FileReader::Prefetch(size_t length, size_t offset, ReadOption option) noexcept { return {FSEC_OK, 0}; }

size_t FileReader::EvaluateCurrentMemUsed() noexcept
{
    switch (GetType()) {
    case FSFT_MEM:
    case FSFT_MMAP_LOCK:
    case FSFT_SLICE:
    case FSFT_RESOURCE:
        return GetLength();
    case FSFT_MMAP:
    case FSFT_BLOCK:
        return 0;
    case FSFT_BUFFERED:
        return std::min(GetLength(), (uint64_t)ReaderOption::DEFAULT_BUFFER_SIZE);
    default:
        return 0;
    }
}

string FileReader::TEST_Load() noexcept(false)
{
    auto len = GetLength();
    unique_ptr<char[]> buf(new char[len]);
    Read(buf.get(), len).GetOrThrow();
    return string(buf.get(), len);
}

string FileReader::DebugString() const noexcept { return GetLogicalPath(); }

FSResult<void> FileReader::Seek(int64_t offset) noexcept
{
    int64_t logicLen = (int64_t)GetLogicLength();
    if (offset > logicLen) {
        AUTIL_LOG(ERROR, "seek file[%s] out of range, offset: [%lu], file length: [%lu]", DebugString().c_str(), offset,
                  logicLen);
        return FSEC_BADARGS;
    }
    _offset = offset;
    return FSEC_OK;
}

bool FileReader::IsMemLock() const noexcept
{
    FSFileType fsType = GetType();
    return fsType == FSFT_MMAP_LOCK || fsType == FSFT_SLICE || fsType == FSFT_MEM || fsType == FSFT_RESOURCE;
}

void FileReader::InitMetricReporter(FileSystemMetricsReporter* reporter) noexcept
{
    std::shared_ptr<FileNode> fileNode = GetFileNode();
    if (fileNode) {
        fileNode->InitMetricReporter(reporter);
        fileNode->UpdateFileNodeUseCount(fileNode.use_count() - 1);
    }
}

}} // namespace indexlib::file_system
