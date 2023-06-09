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
#include "indexlib/index/orc/InputStreamImpl.h"

#include <assert.h>

#include "indexlib/file_system/file/FileReader.h"
#include "orc/Exceptions.hh"

namespace indexlibv2::index {

InputStreamImpl::InputStreamImpl(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader)
    : _fileReader(fileReader)
    , _readBytes(0)
{
}

InputStreamImpl::~InputStreamImpl() {}

uint64_t InputStreamImpl::getLength() const { return _fileReader->GetLength(); }

uint64_t InputStreamImpl::getNaturalReadSize() const { return 0; }

void InputStreamImpl::read(void* buf, uint64_t length, uint64_t offset)
{
    size_t readBytes = 0;
    try {
        readBytes = _fileReader->Read(buf, length, offset).GetOrThrow();
    } catch (const std::exception& e) {
        throw orc::ParseError(e.what());
    }
    if (readBytes != length) {
        throw orc::ParseError("read failed, expected length: " + std::to_string(length) +
                              ", actual read length:" + std::to_string(readBytes));
    } else {
        _readBytes += readBytes;
    }
}

void InputStreamImpl::readAsync(void* buf, uint64_t length, uint64_t offset) { read(buf, length, offset); }

void InputStreamImpl::loadCache(uint64_t offset, uint64_t length) {}

orc::AsyncState InputStreamImpl::waitForAsyncComplete(uint64_t offset, int64_t timeOut)
{
    if (_failedRequests.count(offset) > 0) {
        return orc::ASYNC_FAILED;
    } else {
        return orc::ASYNC_COMPLETED;
    }
}

const std::string& InputStreamImpl::getName() const { return _fileReader->GetLogicalPath(); }

const std::string& InputStreamImpl::getUniqueId() const { return _fileReader->GetLogicalPath(); }

uint64_t InputStreamImpl::GetReadBytes() const { return _readBytes; }

bool InputStreamImpl::isAsyncReadSupported() const { return false; }

uint64_t InputStreamImpl::getPendingRequestCount() const { return 0; }

} // namespace indexlibv2::index
