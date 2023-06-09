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
namespace indexlibv2::index {

// bucket
#pragma pack(push)
#pragma pack(4)
template <typename _KT, typename _VT>
class SpecialValueBucket
{
public:
    SpecialValueBucket() : _key(), _value() {}
    ~SpecialValueBucket() {}

public:
    const _KT& Key() const { return _key; }
    const _VT& Value() const { return _value; }

    bool IsEmpty() const { return _value.IsEmpty(); }
    bool IsEqual(const _KT& key) const { return _key == key; }
    bool IsDeleted() const { return _value.IsDeleted(); }
    std::pair<bool, _VT> DeletedOrValue() const
    {
        auto value = _value;
        return {value.IsDeleted(), value};
    }

    void UpdateValue(const _VT& value) { _value.SetValue(value); }

    void Set(const _KT& key, const _VT& value)
    {
        _key = key;
        _value.SetValue(value);
    }
    void Set(const SpecialValueBucket<_KT, _VT>& other)
    {
        _key = other.Key();
        _value.SetValue(other.Value());
    }
    void SetDelete(const _KT& key, const _VT& value)
    {
        _key = key;
        _value.SetDelete(value);
    }
    void SetEmpty() { _value.SetEmpty(); }

private:
    _KT _key;
    _VT _value;
};
#pragma pack(pop)
} // namespace indexlibv2::index
