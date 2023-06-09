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

#include <cassert>
#include <limits>

#include "indexlib/index/kkv/common/SKeyListInfo.h"

namespace indexlibv2::index {

// for non delete value
template <typename _RealVT>
class NonDeleteValue
{
public:
    NonDeleteValue() : _value(std::numeric_limits<_RealVT>::max()) {}

    NonDeleteValue(const _RealVT& value) : _value(value) {}

public:
    // for User
    using ValueType = _RealVT;
    const _RealVT& Value() const { return _value; }
    _RealVT& Value() { return _value; }

public:
    // for NonDeleteValueBucket
    bool IsEmpty() const { return _value == std::numeric_limits<_RealVT>::max(); }
    bool IsDeleted() const { return false; }
    void SetValue(const NonDeleteValue<_RealVT>& value) { _value = value.Value(); }
    void SetDelete(const NonDeleteValue<_RealVT>& value) { assert(false); }
    void SetEmpty() { _value = std::numeric_limits<_RealVT>::max(); }
    bool operator<(const NonDeleteValue<_RealVT>& other) const { return _value < other._value; }
    bool operator==(const NonDeleteValue<_RealVT>& other) const { return _value == other._value; }

private:
    _RealVT _value;
};

template <>
inline NonDeleteValue<SKeyListInfo>::NonDeleteValue() : _value(SKeyListInfo())
{
}

template <>
inline bool NonDeleteValue<SKeyListInfo>::IsEmpty() const
{
    return _value == SKeyListInfo();
}

template <>
inline void NonDeleteValue<SKeyListInfo>::SetEmpty()
{
    _value = SKeyListInfo();
}

} // namespace indexlibv2::index
