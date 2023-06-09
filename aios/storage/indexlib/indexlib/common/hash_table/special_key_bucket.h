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
#ifndef __INDEXLIB_SPECIAL_KEY_BUCKET_H
#define __INDEXLIB_SPECIAL_KEY_BUCKET_H

#include <cassert>

namespace indexlib { namespace common {

#pragma pack(push)
#pragma pack(4)

template <typename _KT, typename _VT, _KT _EmptyKey = (_KT)0xFFFFFFFFFFFFFEFFUL,
          _KT _DeleteKey = (_KT)0xFFFFFFFFFFFFFDFFUL>
class SpecialKeyBucket
{
    static_assert(std::is_same<_KT, uint64_t>::value || std::is_same<_KT, uint32_t>::value,
                  "key type only support uint64_t or uint32_t,"
                  "due to the default _EmptyKey and _DeleteKey");
    // ATTENTION: can not guarantee concurrent read and write
public:
    SpecialKeyBucket() : mKey(_EmptyKey), mValue() {}
    ~SpecialKeyBucket() {}

public:
    typedef SpecialKeyBucket<_KT, _VT, 0, 1> SpecialBucket;
    static bool IsEmptyKey(const _KT& key) { return key == _EmptyKey; }
    static bool IsDeleteKey(const _KT& key) { return key == _DeleteKey; }
    static _KT EmptyKey() { return _EmptyKey; }
    static _KT DeleteKey() { return _DeleteKey; }

public:
    const _KT& Key() const { return IsDeleted() ? mKeyInValue : mKey; }
    const _VT& Value() const { return mValue; }

    bool IsEmpty() const { return mKey == _EmptyKey; }
    bool IsEqual(const _KT& key) const
    {
        assert(key != _DeleteKey);
        assert(key != _EmptyKey);
        if (mKey == _DeleteKey) {
            return mKeyInValue == key;
        }
        return mKey == key;
    }
    bool IsDeleted() const { return (mKey == _DeleteKey); }
    std::pair<bool, _VT> DeletedOrValue() const { return {IsDeleted(), Value()}; }

    void UpdateValue(const _VT& value) { mValue = value; }
    void Set(const _KT& key, const _VT& value)
    {
        assert(key != _DeleteKey);
        assert(key != _EmptyKey);
        mValue = value;
        mKey = key;
    }
    void Set(const SpecialKeyBucket<_KT, _VT, _EmptyKey, _DeleteKey>& other)
    {
        *this = other; // thread unsafe
    }
    void SetDelete(const _KT& key, const _VT& value)
    {
        assert(mKey == key || mKey == _EmptyKey || mKey == _DeleteKey);
        mKeyInValue = key;
        mKey = _DeleteKey; // thread unsafe
    }
    void SetEmpty()
    {
        mKey = _EmptyKey;
        new (&mValue) _VT;
    }

private:
    _KT mKey;
    union {
        _VT mValue;
        _KT mKeyInValue;
    };
};
#pragma pack(pop)
}} // namespace indexlib::common

#endif //__INDEXLIB_SPECIAL_KEY_BUCKET_H
