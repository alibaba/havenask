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

#include "indexlib/index/aggregation/IValueIterator.h"

namespace indexlibv2::index {

template <typename Filter>
class ValueIteratorWithFilter : public IValueIterator
{
public:
    ValueIteratorWithFilter(std::unique_ptr<IValueIterator> impl, std::unique_ptr<Filter> filter)
        : _impl(std::move(impl))
        , _filter(std::move(filter))
    {
    }

public:
    bool HasNext() const override { return _impl->HasNext(); }
    Status Next(autil::mem_pool::PoolBase* pool, autil::StringView& value) override
    {
        while (_impl->HasNext()) {
            auto s = _impl->Next(pool, value);
            if (!s.IsOK()) {
                return s;
            }
            if (_filter->Filter(value)) {
                return Status::OK();
            }
        }
        return Status::Eof();
    }

private:
    std::unique_ptr<IValueIterator> _impl;
    std::unique_ptr<Filter> _filter;
};

} // namespace indexlibv2::index
