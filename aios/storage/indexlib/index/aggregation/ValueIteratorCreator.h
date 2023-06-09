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

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/aggregation/IValueIterator.h"

namespace indexlibv2::index {
class AggregationReader;

class ValueIteratorCreator
{
public:
    enum IteratorHint { IH_AUTO, IH_SEQ, IH_SORT };

public:
    explicit ValueIteratorCreator(const AggregationReader* reader);
    ~ValueIteratorCreator();

public:
    // range
    ValueIteratorCreator& FilterBy(const std::string& fieldName, const std::string& from, const std::string& to);

    ValueIteratorCreator& Hint(IteratorHint hint);

    StatusOr<std::unique_ptr<IValueIterator>> CreateIterator(uint64_t key, autil::mem_pool::Pool* pool) const;
    StatusOr<std::unique_ptr<IValueIterator>> CreateDistinctIterator(uint64_t key, autil::mem_pool::Pool* pool,
                                                                     const std::string& distinctField) const;

private:
    Status FilterByRange(std::vector<std::unique_ptr<IValueIterator>>& leafIters) const;
    std::unique_ptr<IValueIterator> CreateComposeIterator(std::vector<std::unique_ptr<IValueIterator>> leafIters,
                                                          autil::mem_pool::Pool* pool, bool needSort) const;

private:
    const AggregationReader* _reader = nullptr;

    struct RangeParam {
        std::string fieldName;
        std::string from;
        std::string to;
    };
    std::unique_ptr<RangeParam> _range;
    mutable IteratorHint _hint = IH_AUTO;
};

} // namespace indexlibv2::index
