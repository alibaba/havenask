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
#include "autil/ConstString.h"

namespace indexlibv2::index {

//////////////////////////////////////////////////////////////////////
class ValueUnpacker
{
public:
    ValueUnpacker(size_t bufferSize) : _buffer(new char[bufferSize]) {};
    virtual ~ValueUnpacker()
    {
        if (_buffer) {
            delete[] _buffer;
            _buffer = nullptr;
        }
    };

public:
    virtual void Unpack(const autil::StringView& value, uint64_t& ts, autil::StringView& realValue) const = 0;

    virtual autil::StringView Pack(const autil::StringView& realValue, uint32_t ts) = 0;

    virtual size_t GetValueSize() const = 0;

    void Unpack(const autil::StringView& value, uint32_t& ts, autil::StringView& realValue) const
    {
        uint64_t tmpTs = 0;
        Unpack(value, tmpTs, realValue);
        ts = tmpTs;
    }

    template <typename _RealVT>
    void UnpackRealType(const autil::StringView& value, uint32_t& ts, _RealVT& realValue)
    {
        autil::StringView valueData;
        Unpack(value, ts, valueData);
        assert(sizeof(realValue) == valueData.size());
        realValue = *(reinterpret_cast<_RealVT*>((char*)valueData.data()));
    }

protected:
    char* _buffer;
};

template <typename _RealVT>
class TimestampValueUnpacker final : public ValueUnpacker
{
public:
    TimestampValueUnpacker() : ValueUnpacker(sizeof(uint32_t) + sizeof(_RealVT)) {}

private:
    static const uint32_t TimestampMask = 0x7FFFFFFF;

public:
    void Unpack(const autil::StringView& value, uint64_t& ts, autil::StringView& realValue) const override final
    {
        uint32_t timestamp = *(int32_t*)value.data();
        ts = timestamp & TimestampMask;
        realValue = autil::StringView(value.data() + 4, sizeof(_RealVT));
    }

    autil::StringView Pack(const autil::StringView& realValue, uint32_t ts) override final;

    size_t GetValueSize() const override { return sizeof(_RealVT); }
};

template <typename _RealVT>
class Timestamp0ValueUnpacker : public ValueUnpacker
{
public:
    Timestamp0ValueUnpacker() : ValueUnpacker(sizeof(_RealVT)) {}

public:
    void Unpack(const autil::StringView& value, uint64_t& ts, autil::StringView& realValue) const override final
    {
        ts = 0;
        realValue = autil::StringView(value.data(), sizeof(_RealVT));
    }
    autil::StringView Pack(const autil::StringView& realValue, uint32_t ts) override final;

    size_t GetValueSize() const override { return sizeof(_RealVT); }
};

// for timestamp value
#pragma pack(push)
#pragma pack(4)
template <typename _RealVT>
class TimestampValue
{
private:
    // Empty: 0x7FFFFFFF, Valid: 0-, Deleted: 1-
    static const uint32_t Empty = 0x7FFFFFFF;
    static const uint32_t DeleteMask = 0x80000000;
    static const uint32_t TimestampMask = 0x7FFFFFFF;

public:
    TimestampValue() : _timestamp(Empty), _value()
    {
        assert(IsEmpty());
        assert(!IsDeleted());
    }
    explicit TimestampValue(uint32_t timestamp) : _timestamp(timestamp | DeleteMask), _value()
    {
        assert(!IsEmpty());
        assert(IsDeleted());
        assert(Timestamp() == timestamp);
    }

    explicit TimestampValue(uint32_t timestamp, const _RealVT& value)
        : _timestamp(timestamp & TimestampMask)
        , _value(value)
    {
        assert(!IsEmpty());
        assert(!IsDeleted());
        assert(Timestamp() == timestamp);
    }

public:
    // for User
    typedef TimestampValueUnpacker<_RealVT> ValueUnpackerType;
    typedef _RealVT ValueType;
    static ValueUnpacker* CreateValueUnpacker() { return new ValueUnpackerType(); }
    uint32_t Timestamp() const { return _timestamp & TimestampMask; }
    const _RealVT& Value() const { return _value; }
    _RealVT& Value() { return _value; }

public:
    // for SpecialValueBucket
    bool IsEmpty() const { return _timestamp == Empty; }
    bool IsDeleted() const { return _timestamp & DeleteMask; }
    void SetValue(const TimestampValue<_RealVT>& value)
    {
        if (!IsEmpty() && !IsDeleted()) {
            SetDelete(value); // defend for value cross border of 8Bytes
        }
        _value = value.Value();
        volatile uint32_t tempTimestamp = value.Timestamp();
        _timestamp = tempTimestamp;
    }
    void SetDelete(const TimestampValue<_RealVT>& value)
    {
        volatile uint32_t tempTimestamp = value.Timestamp() | DeleteMask;
        _timestamp = tempTimestamp;
    }
    void SetEmpty()
    {
        _timestamp = Empty;
        new (&_value) _RealVT;
    }
    bool operator<(const TimestampValue<_RealVT>& other) const
    {
        if (IsDeleted()) {
            return other.IsDeleted() ? Timestamp() < other.Timestamp() : true;
        }
        if (other.IsDeleted()) {
            return false;
        }
        return _value < other._value;
    }

    bool operator==(const TimestampValue<_RealVT>& other) const
    {
        return _timestamp == other._timestamp && (IsDeleted() || _value == other._value);
    }

private:
    uint32_t _timestamp; // max(0x7FFFFFFF-1) is 01/19/2038 11:14:06
    _RealVT _value;
};
#pragma pack(pop)

template <typename _RealVT>
autil::StringView TimestampValueUnpacker<_RealVT>::Pack(const autil::StringView& realValue, uint32_t ts)
{
    if (realValue.empty()) {
        TimestampValue<_RealVT> value(ts, _RealVT());
        *(TimestampValue<_RealVT>*)_buffer = value;
        return autil::StringView(_buffer, sizeof(value));
    }
    TimestampValue<_RealVT> value(ts, *(_RealVT*)realValue.data());
    *(TimestampValue<_RealVT>*)_buffer = value;
    return autil::StringView(_buffer, sizeof(value));
}

//////////////////////////////////////////////////////////////////////
// for no timestamp value, will use special key bucket
template <typename _RealVT>
class Timestamp0Value
{
public:
    Timestamp0Value() : _value() {}
    explicit Timestamp0Value(uint32_t timestamp, const _RealVT& value = _RealVT()) : _value(value) {}

public:
    // for User
    typedef Timestamp0ValueUnpacker<_RealVT> ValueUnpackerType;
    typedef _RealVT ValueType;
    uint32_t Timestamp() const { return 0; }
    const _RealVT& Value() const { return _value; }
    _RealVT& Value() { return _value; }
    static ValueUnpacker* CreateValueUnpacker() { return new Timestamp0ValueUnpacker<_RealVT>(); }

public:
    // for SpecialKeyBucket
    bool operator<(const Timestamp0Value<_RealVT>& other) const { return _value < other._value; }
    bool operator==(const Timestamp0Value<_RealVT>& other) const { return _value == other._value; }

private:
    _RealVT _value;
};

template <typename _RealVT>
autil::StringView Timestamp0ValueUnpacker<_RealVT>::Pack(const autil::StringView& realValue, uint32_t ts)
{
    if (realValue.empty()) {
        Timestamp0Value<_RealVT> value;
        *(Timestamp0Value<_RealVT>*)_buffer = value;
        return autil::StringView(_buffer, sizeof(value));
    }
    Timestamp0Value<_RealVT> value(ts, *(_RealVT*)realValue.data());
    *(Timestamp0Value<_RealVT>*)_buffer = value;
    return autil::StringView(_buffer, sizeof(value));
}

// inline void UnpackTimestampValue(bool hasTimestamp, const autil::StringView& originValue,
//                                  uint64_t& ts, autil::StringView& realValue) {
//     if (!hasTimestamp) {
//         ts = 0;
//         realValue = originValue;
//     } else {
//         ts = *(uint32_t*)originValue.data();
//         realValue = autil::StringView(originValue.data() + 4, originValue.size() - 4);
//     }
// }
//////////////////////////////////////////////////////////////////////
// for offset value
template <typename _RealVT, _RealVT _EmptyValue = std::numeric_limits<_RealVT>::max(),
          _RealVT _DeleteValue = std::numeric_limits<_RealVT>::max() - 1>
class SpecialValue
{
public:
    SpecialValue() : _value(_EmptyValue) {}
    SpecialValue(const _RealVT& value) : _value(value) {}

public:
    // for User
    typedef _RealVT ValueType;
    const _RealVT& Value() const { return _value; }
    _RealVT& Value() { return _value; }

public:
    // for SpecialValueBucket
    bool IsEmpty() const { return _value == _EmptyValue; }
    bool IsDeleted() const { return _value == _DeleteValue; }
    void SetValue(const SpecialValue<_RealVT, _EmptyValue, _DeleteValue>& value) { _value = value.Value(); }
    void SetDelete(const SpecialValue<_RealVT, _EmptyValue, _DeleteValue>& value) { _value = _DeleteValue; }
    void SetEmpty() { _value = _EmptyValue; }
    bool operator<(const SpecialValue<_RealVT, _EmptyValue, _DeleteValue>& other) const
    {
        return _value < other._value;
    }
    bool operator==(const SpecialValue<_RealVT, _EmptyValue, _DeleteValue>& other) const
    {
        return _value == other._value;
    }

private:
    _RealVT _value;
};

#pragma pack(push)
#pragma pack(4)

// for short offset value
template <typename _RealVT, _RealVT _EmptyValue = std::numeric_limits<_RealVT>::max(),
          _RealVT _DeleteValue = std::numeric_limits<_RealVT>::max() - 1>
class OffsetValue
{
public:
    OffsetValue() : _value(_EmptyValue) {}
    OffsetValue(const _RealVT& value) : _value(value) {}
    explicit OffsetValue(uint32_t timestamp, const _RealVT& value) : _value(value) {}

public:
    // for User
    typedef Timestamp0ValueUnpacker<_RealVT> ValueUnpackerType;
    typedef _RealVT ValueType;
    const _RealVT& Value() const { return _value; }
    _RealVT& Value() { return _value; }
    uint32_t Timestamp() const { return 0; }
    static ValueUnpacker* CreateValueUnpacker() { return new Timestamp0ValueUnpacker<_RealVT>(); }

public:
    // for OffsetValueBucket
    bool IsEmpty() const { return _value == _EmptyValue; }
    bool IsDeleted() const { return _value == _DeleteValue; }
    void SetValue(const OffsetValue<_RealVT, _EmptyValue, _DeleteValue>& value) { _value = value.Value(); }
    void SetDelete(const OffsetValue<_RealVT, _EmptyValue, _DeleteValue>& value) { _value = _DeleteValue; }
    void SetEmpty() { _value = _EmptyValue; }
    bool operator<(const OffsetValue<_RealVT, _EmptyValue, _DeleteValue>& other) const { return _value < other._value; }
    bool operator==(const OffsetValue<_RealVT, _EmptyValue, _DeleteValue>& other) const
    {
        return _value == other._value;
    }

private:
    _RealVT _value;
};

#pragma pack(pop)
} // namespace indexlibv2::index
