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

#include "indexlib/index/aggregation/ISegmentReader.h"

namespace indexlibv2::index {
struct ReadWriteState;
class PostingMap;

class AggregationMemReader : public ISegmentReader
{
public:
    AggregationMemReader(const std::shared_ptr<const ReadWriteState>& state,
                         const std::shared_ptr<const PostingMap>& data);
    ~AggregationMemReader();

public:
    std::unique_ptr<IKeyIterator> CreateKeyIterator() const override;
    std::unique_ptr<IValueIterator> Lookup(uint64_t key, autil::mem_pool::PoolBase* pool) const override;
    bool GetValueMeta(uint64_t key, ValueMeta& meta) const override;

private:
    std::shared_ptr<const ReadWriteState> _state;
    std::shared_ptr<const PostingMap> _data;
};

} // namespace indexlibv2::index
