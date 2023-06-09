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
#include <queue>
#include <vector>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/aggregation/IValueIterator.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueComparator.h"

namespace indexlibv2::index {

class SortedMultiSegmentValueIterator final : public IValueIterator
{
public:
    explicit SortedMultiSegmentValueIterator(std::shared_ptr<PackValueComparator> cmp);
    ~SortedMultiSegmentValueIterator();

public:
    bool HasNext() const override;
    Status Next(autil::mem_pool::PoolBase* pool, autil::StringView& value) override;

public:
    static std::unique_ptr<IValueIterator> Create(std::vector<std::unique_ptr<IValueIterator>> iters,
                                                  autil::mem_pool::PoolBase* pool,
                                                  std::shared_ptr<PackValueComparator> cmp);

private:
    Status Init(std::vector<std::unique_ptr<IValueIterator>> iters, autil::mem_pool::PoolBase* pool);

private:
    using Item = std::pair<autil::StringView, size_t>;
    struct Comparator {
        PackValueComparator* impl = nullptr;
        bool operator()(const Item& lhs, const Item& rhs) const { return !LessThan(lhs, rhs); }
        bool LessThan(const Item& lhs, const Item& rhs) const
        {
            int ret = impl->Compare(lhs.first, rhs.first);
            if (ret == 0) {
                return lhs.second < rhs.second;
            } else {
                return ret < 0;
            }
        }
    };
    using Heap = std::priority_queue<Item, std::vector<Item>, Comparator>;

private:
    std::shared_ptr<PackValueComparator> _cmp;
    std::vector<std::unique_ptr<IValueIterator>> _iters;
    Heap _heap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
