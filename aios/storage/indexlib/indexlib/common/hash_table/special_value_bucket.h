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

namespace indexlib { namespace common {

// bucket
#pragma pack(push)
#pragma pack(4)
template <typename _KT, typename _VT>
class SpecialValueBucket
{
public:
    SpecialValueBucket() : mKey(), mValue() {}
    ~SpecialValueBucket() {}

public:
    const _KT& Key() const { return mKey; }
    const _VT& Value() const { return mValue; }

    bool IsEmpty() const { return mValue.IsEmpty(); }
    bool IsEqual(const _KT& key) const { return mKey == key; }
    bool IsDeleted() const { return mValue.IsDeleted(); }
    std::pair<bool, _VT> DeletedOrValue() const
    {
        auto value = mValue;
        return {value.IsDeleted(), value};
    }

    void UpdateValue(const _VT& value) { mValue.SetValue(value); }

    void Set(const _KT& key, const _VT& value)
    {
        mKey = key;
        mValue.SetValue(value);
    }
    void Set(const SpecialValueBucket<_KT, _VT>& other)
    {
        mKey = other.Key();
        mValue.SetValue(other.Value());
    }
    void SetDelete(const _KT& key, const _VT& value)
    {
        mKey = key;
        mValue.SetDelete(value);
    }
    void SetEmpty() { mValue.SetEmpty(); }

private:
    _KT mKey;
    _VT mValue;
};
#pragma pack(pop)
}} // namespace indexlib::common
