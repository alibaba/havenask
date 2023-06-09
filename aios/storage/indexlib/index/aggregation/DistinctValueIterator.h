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
#include <unordered_set>

#include "autil/StringView.h"
#include "indexlib/index/aggregation/IValueIterator.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReferenceTyped.h"

namespace indexlibv2::index {
namespace __detail {
template <typename T>
class UnderlyingSorted
{
public:
    bool Update(T value)
    {
        if (!_lastValue || _lastValue.value() != value) {
            _lastValue = value;
            return true;
        }
        return false;
    }

private:
    std::optional<T> _lastValue;
};

template <typename T>
class UnderlyingUnsorted
{
public:
    bool Update(T value)
    {
        auto [_, ret] = _seenValues.insert(value);
        return ret;
    }

private:
    std::unordered_set<T> _seenValues;
};

template <typename T, typename DistinctState>
class DistinctValueIterator final : public IValueIterator
{
public:
    DistinctValueIterator(std::unique_ptr<IValueIterator> impl, const AttributeReferenceTyped<T>* distFieldRef)
        : _impl(std::move(impl))
        , _distFieldRef(distFieldRef)
    {
    }

public:
    bool HasNext() const override { return _impl->HasNext(); }
    Status Next(autil::mem_pool::PoolBase* pool, autil::StringView& value) override
    {
        while (HasNext()) {
            auto s = _impl->Next(pool, value);
            if (!s.IsOK()) {
                return s;
            }
            T distValue;
            _distFieldRef->GetValue(value.data(), distValue, nullptr);
            if (_state.Update(distValue)) {
                return Status::OK();
            }
        }
        return Status::Eof();
    }

private:
    std::unique_ptr<IValueIterator> _impl;
    const AttributeReferenceTyped<T>* _distFieldRef;
    DistinctState _state;
};
} // namespace __detail

template <typename T>
using DistinctSortedValueIterator = __detail::DistinctValueIterator<T, __detail::UnderlyingSorted<T>>;

template <typename T>
using DistinctUnsortedValueIterator = __detail::DistinctValueIterator<T, __detail::UnderlyingUnsorted<T>>;

} // namespace indexlibv2::index
