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
#include "indexlib/index/aggregation/IValueIterator.h"
#include "indexlib/index/aggregation/ValueStat.h"

namespace indexlib::file_system {
class FileWriter;
}

namespace indexlibv2::index {

class IValueList
{
public:
    virtual ~IValueList() = default;

public:
    virtual Status Init() = 0;
    virtual Status Append(const autil::StringView& data) = 0;
    virtual size_t GetTotalCount() const = 0;
    virtual size_t GetTotalBytes() const = 0;
    virtual std::unique_ptr<IValueIterator> CreateIterator(autil::mem_pool::PoolBase* pool) const = 0;
    virtual StatusOr<ValueStat> Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& file,
                                     autil::mem_pool::PoolBase* dumpPool) = 0;
};

} // namespace indexlibv2::index
