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

#include "autil/StringView.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReferenceTyped.h"

namespace indexlibv2::index {

class CountFunctor
{
public:
    using ResultType = uint64_t;

public:
    void operator()(const autil::StringView& /*unused*/) { ++_count; }
    ResultType GetResult() const { return _count; }

private:
    uint64_t _count = 0;
};

template <typename T, typename OutputType>
class SumFunctor
{
public:
    using ResultType = OutputType;

public:
    explicit SumFunctor(const AttributeReferenceTyped<T>* ref) : _ref(ref) {}

public:
    void operator()(const autil::StringView& value)
    {
        T val = T();
        _ref->GetValue(value.data(), val, nullptr);
        _sum += val;
    }

    ResultType GetResult() const { return _sum; }

private:
    const AttributeReferenceTyped<T>* _ref;
    OutputType _sum {0};
};

// TODO: support other function, such as min/max

} // namespace indexlibv2::index
