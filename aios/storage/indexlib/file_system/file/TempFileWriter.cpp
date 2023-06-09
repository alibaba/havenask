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
#include "indexlib/file_system/file/TempFileWriter.h"

#include "indexlib/util/Exception.h"
#include "indexlib/util/IoExceptionController.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, TempFileWriter);

TempFileWriter::TempFileWriter(const std::shared_ptr<FileWriter>& writer, CloseFunc&& closeFunc) noexcept
    : _isClosed(false)
    , _writer(writer)
    , _closeFunc(closeFunc)
{
}

TempFileWriter::~TempFileWriter() noexcept
{
    if (!_isClosed) {
        AUTIL_LOG(ERROR, "temp file [%s] not CLOSED in destructor. Exception flag [%d]", GetLogicalPath().c_str(),
                  util::IoExceptionController::HasFileIOException());
        assert(util::IoExceptionController::HasFileIOException());
    }
}

FSResult<void> TempFileWriter::Open(const std::string& logicalPath, const std::string& physicalPath) noexcept
{
    return _writer->Open(logicalPath, physicalPath);
}

FSResult<void> TempFileWriter::Close() noexcept
{
    _isClosed = true;
    RETURN_IF_FS_ERROR(_writer->Close(), "close writer failed");
    _closeFunc(util::PathUtil::GetFileName(_writer->GetLogicalPath()));
    return FSEC_OK;
}

FSResult<size_t> TempFileWriter::Write(const void* buffer, size_t length) noexcept
{
    return _writer->Write(buffer, length);
}

size_t TempFileWriter::GetLength() const noexcept { return _writer->GetLength(); }

const std::string& TempFileWriter::GetLogicalPath() const noexcept { return _writer->GetLogicalPath(); }
const std::string& TempFileWriter::GetPhysicalPath() const noexcept { return _writer->GetPhysicalPath(); }
const WriterOption& TempFileWriter::GetWriterOption() const noexcept { return _writer->GetWriterOption(); }

FSResult<void> TempFileWriter::ReserveFile(size_t reserveSize) noexcept { return _writer->ReserveFile(reserveSize); }
FSResult<void> TempFileWriter::Truncate(size_t truncateSize) noexcept { return _writer->Truncate(truncateSize); }

void* TempFileWriter::GetBaseAddress() noexcept { return _writer->GetBaseAddress(); }

} // namespace indexlib::file_system
