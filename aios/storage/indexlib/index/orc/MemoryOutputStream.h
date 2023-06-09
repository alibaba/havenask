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
#pragma once

#include <string>

#include "indexlib/file_system/ByteSliceWriter.h"
#include "orc/OrcFile.hh"

namespace indexlibv2::index {

constexpr uint64_t kOrcNaturalWriteSize = 128 * 1024;

class MemoryOutputStream : public ::orc::OutputStream
{
public:
    explicit MemoryOutputStream(indexlib::file_system::ByteSliceWriter* memWriter)
        : _name("MemoryOutputStream")
        , _memWriter(memWriter)
    {
    }
    uint64_t getLength() const override { return _memWriter->GetSize(); }
    uint64_t getNaturalWriteSize() const override { return kOrcNaturalWriteSize; }
    // TODO(yonghao.fyh) : write to file system
    void write(const void* buf, size_t size) override { _memWriter->Write(buf, size); }
    void write(const void* buf, size_t length, uint64_t crc32) override { write(buf, length); }
    const std::string& getName() const override { return _name; }
    void close() override {}

private:
    std::string _name;
    indexlib::file_system::ByteSliceWriter* _memWriter;
};

} // namespace indexlibv2::index
