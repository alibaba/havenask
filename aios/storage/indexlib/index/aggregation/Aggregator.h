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

#include <optional>

#include "autil/mem_pool/Pool.h"
#include "autil/result/Result.h"
#include "indexlib/index/aggregation/AggregationReader.h"
#include "indexlib/index/aggregation/ValueIteratorCreator.h"

namespace indexlibv2::index {

class Aggregator
{
public:
    explicit Aggregator(const AggregationReader* reader, autil::mem_pool::Pool* pool);
    ~Aggregator();

public:
    Aggregator& Key(uint64_t key);
    Aggregator& Keys(const std::vector<autil::StringView>& rawKeys);
    Aggregator& Keys(const std::vector<std::string>& rawKeys);
    Aggregator& FilterBy(const std::string& field, const std::string& from, const std::string& to);

public:
    autil::Result<uint64_t> Count() const;
    autil::Result<uint64_t> DistinctCount(const std::string& fieldName) const;
    autil::Result<double> Sum(const std::string& fieldName) const;
    autil::Result<double> DistinctSum(const std::string& sumField, const std::string& distinctField) const;

private:
    autil::Result<double> SumImpl(std::unique_ptr<IValueIterator> iter, const std::string& fieldName) const;

    template <typename Functor>
    autil::Result<typename Functor::ResultType> Aggregate(std::unique_ptr<IValueIterator> iter, Functor f) const;

private:
    const AggregationReader* _reader = nullptr;
    autil::mem_pool::Pool* _pool = nullptr;
    uint64_t _key = 0;
    std::unique_ptr<ValueIteratorCreator> _iterCreator;
};

} // namespace indexlibv2::index
