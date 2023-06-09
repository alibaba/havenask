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
#include "indexlib/index/orc/OutputStreamImpl.h"

#include "indexlib/file_system/file/FileWriter.h"

namespace indexlibv2::index {

OutputStreamImpl::OutputStreamImpl(const std::shared_ptr<indexlib::file_system::FileWriter>& fileWriter)
    : _fileWriter(fileWriter)
{
}

OutputStreamImpl::~OutputStreamImpl() {}

uint64_t OutputStreamImpl::getLength() const { return _fileWriter->GetLength(); }

uint64_t OutputStreamImpl::getNaturalWriteSize() const { return _fileWriter->GetWriterOption().bufferSize; }

void OutputStreamImpl::write(const void* buf, size_t length) { (void)_fileWriter->Write(buf, length); }

void OutputStreamImpl::write(const void* buf, size_t length, uint64_t crc32)
{
    write(buf, length);
    // ignore crc32
    (void)crc32;
}

const std::string& OutputStreamImpl::getName() const { return _fileWriter->GetPhysicalPath(); }

void OutputStreamImpl::close() { _fileWriter->Close().GetOrThrow(); }

void OutputStreamImpl::commit() {}

} // namespace indexlibv2::index
