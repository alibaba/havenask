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

namespace indexlibv2::index {

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
    SpecialKeyBucket() : _key(_EmptyKey), _value() {}
    ~SpecialKeyBucket() {}

public:
    typedef SpecialKeyBucket<_KT, _VT, 0, 1> SpecialBucket;
    static bool IsEmptyKey(const _KT& key) { return key == _EmptyKey; }
    static bool IsDeleteKey(const _KT& key) { return key == _DeleteKey; }
    static _KT EmptyKey() { return _EmptyKey; }
    static _KT DeleteKey() { return _DeleteKey; }

public:
    const _KT& Key() const { return IsDeleted() ? _keyInValue : _key; }
    const _VT& Value() const { return _value; }

    bool IsEmpty() const { return _key == _EmptyKey; }
    bool IsEqual(const _KT& key) const
    {
        assert(key != _DeleteKey);
        assert(key != _EmptyKey);
        if (_key == _DeleteKey) {
            return _keyInValue == key;
        }
        return _key == key;
    }
    bool IsDeleted() const { return (_key == _DeleteKey); }
    std::pair<bool, _VT> DeletedOrValue() const { return {IsDeleted(), Value()}; }

    void UpdateValue(const _VT& value) { _value = value; }
    void Set(const _KT& key, const _VT& value)
    {
        assert(key != _DeleteKey);
        assert(key != _EmptyKey);
        _value = value;
        _key = key;
    }
    void Set(const SpecialKeyBucket<_KT, _VT, _EmptyKey, _DeleteKey>& other)
    {
        *this = other; // thread unsafe
    }
    void SetDelete(const _KT& key, const _VT& value)
    {
        assert(_key == key || _key == _EmptyKey || _key == _DeleteKey);
        _keyInValue = key;
        _key = _DeleteKey; // thread unsafe
    }
    void SetEmpty()
    {
        _key = _EmptyKey;
        new (&_value) _VT;
    }

private:
    _KT _key;
    union {
        _VT _value;
        _KT _keyInValue;
    };
};
#pragma pack(pop)
} // namespace indexlibv2::index
