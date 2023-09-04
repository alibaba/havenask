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

#include "indexlib/index/kv/ValueWriter.h"

namespace indexlib::file_system {
class Directory;
class FileWriter;
} // namespace indexlib::file_system

namespace indexlibv2::index {

class DFSValueWriter final : public ValueWriter
{
public:
    explicit DFSValueWriter(std::shared_ptr<indexlib::file_system::FileWriter> file);
    ~DFSValueWriter();

public:
    bool IsFull() const override { return false; }
    Status Write(const autil::StringView& data) override;
    Status Dump(const std::shared_ptr<indexlib::file_system::Directory>& directory) override;
    const char* GetBaseAddress() const override;
    int64_t GetLength() const override;
    void FillStatistics(SegmentStatistics& stat) const override;
    void FillMemoryUsage(MemoryUsage& memUsage) const override;

private:
    std::shared_ptr<indexlib::file_system::FileWriter> _file;
    size_t _originalLength;
    size_t _compressedLength;
};

} // namespace indexlibv2::index
