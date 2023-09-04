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

#include "autil/StringView.h"
#include "indexlib/base/Status.h"

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlib::file_system {
class Directory;
struct WriterOption;
} // namespace indexlib::file_system

namespace indexlibv2::index {
struct MemoryUsage;
struct SegmentStatistics;

class ValueWriter
{
public:
    virtual ~ValueWriter();

public:
    virtual bool IsFull() const = 0;
    virtual Status Write(const autil::StringView& data) = 0;
    virtual Status Dump(const std::shared_ptr<indexlib::file_system::Directory>& directory) = 0;
    virtual const char* GetBaseAddress() const = 0;
    virtual int64_t GetLength() const = 0;
    virtual void FillStatistics(SegmentStatistics& stat) const = 0;
    virtual void FillMemoryUsage(MemoryUsage& memUsage) const = 0;

public:
    static void FillWriterOption(const indexlibv2::config::KVIndexConfig& config,
                                 indexlib::file_system::WriterOption& option);
};

} // namespace indexlibv2::index
