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

#include <unordered_set>

#include "autil/StringView.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReferenceTyped.h"

namespace indexlibv2::index {
namespace __detail {
template <typename T, bool IN>
class InNotInFilter
{
public:
    InNotInFilter(const AttributeReferenceTyped<T>* ref, std::unordered_set<T> testingSet)
        : _ref(ref)
        , _testingSet(std::move(testingSet))
    {
    }

public:
    bool Filter(const autil::StringView& data) const
    {
        T val = T();
        _ref->GetValue(data.data(), val, nullptr);
        bool exist = _testingSet.count(val) > 0;
        if constexpr (IN) {
            return exist;
        } else {
            return !exist;
        }
    }

private:
    const AttributeReferenceTyped<T>* _ref;
    std::unordered_set<T> _testingSet;
};

template <typename T>
class RangeFilter
{
public:
    RangeFilter(const AttributeReferenceTyped<T>* ref, const T& from, const T& to) : _ref(ref), _from(from), _to(to) {}

public:
    bool Filter(const autil::StringView& data) const
    {
        T val = T();
        _ref->GetValue(data.data(), val, nullptr);
        return (_from <= val && val <= _to);
    }

private:
    const AttributeReferenceTyped<T>* _ref;
    T _from;
    T _to;
};
} // namespace __detail

template <typename T>
using In = __detail::InNotInFilter<T, true>;

template <typename T>
using NotIn = __detail::InNotInFilter<T, false>;

template <typename T>
using Range = __detail::RangeFilter<T>;

} // namespace indexlibv2::index
