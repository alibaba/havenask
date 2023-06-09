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

#include <memory>

#include "orc/OrcFile.hh"

namespace indexlib { namespace file_system {
class FileWriter;
}} // namespace indexlib::file_system

namespace indexlibv2::index {

class OutputStreamImpl : public ::orc::OutputStream
{
public:
    explicit OutputStreamImpl(const std::shared_ptr<indexlib::file_system::FileWriter>& fileWriter);
    ~OutputStreamImpl();

public:
    uint64_t getLength() const override;
    uint64_t getNaturalWriteSize() const override;
    void write(const void* buf, size_t length) override;
    void write(const void* buf, size_t length, uint64_t crc32) override;
    const std::string& getName() const override;
    void close() override;
    void commit() override;

private:
    std::shared_ptr<indexlib::file_system::FileWriter> _fileWriter;
};

} // namespace indexlibv2::index
