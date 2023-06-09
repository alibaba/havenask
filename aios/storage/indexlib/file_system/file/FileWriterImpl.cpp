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
#include "indexlib/file_system/file/FileWriterImpl.h"

using namespace std;

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileWriterImpl);

FileWriterImpl::FileWriterImpl() noexcept {}

FileWriterImpl::~FileWriterImpl() noexcept {}

FileWriterImpl::FileWriterImpl(const UpdateFileSizeFunc&& updateFileSizeFunc) noexcept
    : _updateFileSizeFunc(std::move(updateFileSizeFunc))
{
}
FileWriterImpl::FileWriterImpl(const WriterOption& writerOption, const UpdateFileSizeFunc&& updateFileSizeFunc) noexcept
    : _writerOption(writerOption)
    , _updateFileSizeFunc(std::move(updateFileSizeFunc))
{
}

FSResult<void> FileWriterImpl::Open(const std::string& logicalPath, const std::string& physicalPath) noexcept
{
    _logicalPath = logicalPath;
    _physicalPath = physicalPath;
    RETURN_IF_FS_ERROR(DoOpen(), "DoOpen [%s] ==> [%s] failed", logicalPath.c_str(), physicalPath.c_str());
    return FSEC_OK;
}

FSResult<void> FileWriterImpl::Close() noexcept
{
    auto ret = DoClose();
    if (_updateFileSizeFunc) {
        _updateFileSizeFunc(GetLength());
    }
    return ret;
}

} // namespace indexlib::file_system
