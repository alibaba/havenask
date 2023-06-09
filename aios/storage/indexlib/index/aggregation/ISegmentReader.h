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

#include "indexlib/index/aggregation/IKeyIterator.h"
#include "indexlib/index/aggregation/IValueIterator.h"
#include "indexlib/index/aggregation/KeyMeta.h"

namespace indexlibv2::index {

class ISegmentReader
{
public:
    virtual ~ISegmentReader() = default;

public:
    virtual std::unique_ptr<IKeyIterator> CreateKeyIterator() const = 0;
    virtual std::unique_ptr<IValueIterator> Lookup(uint64_t key, autil::mem_pool::PoolBase* pool) const = 0;
    virtual bool GetValueMeta(uint64_t key, ValueMeta& meta) const = 0;
};

} // namespace indexlibv2::index
