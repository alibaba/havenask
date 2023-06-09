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
#include "indexlib/file_system/package/BufferedPackageFileWriter.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/ThreadPool.h"
//#include "indexlib/file_system/file/BufferedFileOutputStream.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileBuffer.h"
#include "indexlib/file_system/package/PackageDiskStorage.h"
#include "indexlib/util/IoExceptionController.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BufferedPackageFileWriter);

BufferedPackageFileWriter::BufferedPackageFileWriter(PackageDiskStorage* packageStorage, size_t packageUnitId,
                                                     const WriterOption& writerOption,
                                                     UpdateFileSizeFunc&& updateFileSizeFunc)
    : BufferedFileWriter(writerOption, std::move(updateFileSizeFunc))
    , _storage(packageStorage)
    , _unitId(packageUnitId)
    , _physicalFileId(-1)
{
}

BufferedPackageFileWriter::~BufferedPackageFileWriter()
{
    if (!_isClosed) {
        AUTIL_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]", _logicalPath.c_str(),
                  util::IoExceptionController::HasFileIOException());
        assert(util::IoExceptionController::HasFileIOException());
    } else {
        assert(!_stream);
        assert(!_buffer);
        assert(!_switchBuffer);
    }
}

ErrorCode BufferedPackageFileWriter::DoOpen() noexcept
{
    _isClosed = false;
    return FSEC_OK;
}

ErrorCode BufferedPackageFileWriter::DoClose() noexcept
{
    if (_isClosed) {
        return FSEC_OK;
    }
    _isClosed = true;
    if (!_stream) {
        // not flushed yet, will flush in PackageDiskStorage.Flush()
        RETURN_IF_FS_ERROR(_storage->StoreLogicalFile(_unitId, _logicalPath, std::move(_buffer)),
                           "StoreLogicalFile failed");
        _buffer = nullptr;
        _switchBuffer = nullptr;
    } else {
        RETURN_IF_FS_ERROR(DumpBuffer(), "DumpBuffer failed");
        if (_switchBuffer) {
            _switchBuffer->Wait();
        }
        if (_threadPool) {
            RETURN_IF_FS_EXCEPTION(_threadPool->checkException(), "checkException failed");
        }
        RETURN_IF_FS_ERROR(_storage->StoreLogicalFile(_unitId, _logicalPath, _physicalFileId, _length),
                           "StoreLogicalFile failed");
        _stream.reset();
        // DELETE_AND_SET_NULL(_buffer);
        // DELETE_AND_SET_NULL(_switchBuffer);
        _buffer = nullptr;
        _switchBuffer = nullptr;
    }
    return FSEC_OK;
}

FSResult<void> BufferedPackageFileWriter::DumpBuffer() noexcept
{
    if (!_stream) {
        ErrorCode ec = FSEC_UNKNOWN;
        std::tie(ec, _physicalFileId, _stream) = _storage->AllocatePhysicalStream(_unitId, GetLogicalPath());
        RETURN_IF_FS_ERROR(ec, "AllocatePhysicalStream failed");
    }

    return BufferedFileWriter::DumpBuffer();
}
}} // namespace indexlib::file_system
