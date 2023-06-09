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

#include <list>
#include <memory>

#include "indexlib/base/Status.h"
#include "indexlib/index/orc/IOrcIterator.h"

namespace future_lite {
class Executor;
}

namespace orc {
class MemoryPool;
}

namespace indexlibv2::index {

struct ReaderOptions {
    ReaderOptions();
    ~ReaderOptions();

    bool async;
    uint32_t batchSize;
    std::list<uint64_t> fieldIds; // orc RowReader use std::list
    std::shared_ptr<orc::MemoryPool> pool;
    future_lite::Executor* executor;

    std::string DebugString() const;
};

class IOrcReader
{
public:
    virtual ~IOrcReader() = default;

public:
    virtual Status CreateIterator(const ReaderOptions& opt, std::unique_ptr<IOrcIterator>& orcIter) = 0;

    virtual uint64_t GetTotalRowCount() const = 0;
    virtual size_t EvaluateCurrentMemUsed() const = 0;
};

} // namespace indexlibv2::index
