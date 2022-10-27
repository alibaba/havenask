#ifndef __INDEXLIB_SPECIAL_VALUE_H
#define __INDEXLIB_SPECIAL_VALUE_H

IE_NAMESPACE_BEGIN(common);

//////////////////////////////////////////////////////////////////////
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
    TimestampValue() : mTimestamp(Empty), mValue()
    {
        assert(IsEmpty());
        assert(!IsDeleted());
    }
    explicit TimestampValue(uint32_t timestamp)
        : mTimestamp(timestamp | DeleteMask)
        , mValue()
    {
        assert(!IsEmpty());
        assert(IsDeleted());
        assert(Timestamp() == timestamp);
    }

    explicit TimestampValue(uint32_t timestamp, const _RealVT& value)
        : mTimestamp(timestamp & TimestampMask)
        , mValue(value)
    {
        assert(!IsEmpty());
        assert(!IsDeleted());
        assert(Timestamp() == timestamp);
    }

public:
    // for User
    typedef _RealVT ValueType;
    uint32_t Timestamp() const { return mTimestamp & TimestampMask;}
    const _RealVT& Value() const { return mValue; }
    _RealVT& Value() { return mValue; }

public:
    // for SpecialValueBucket
    bool IsEmpty() const { return mTimestamp == Empty; }
    bool IsDeleted() const { return mTimestamp & DeleteMask; }
    void SetValue(const TimestampValue<_RealVT>& value)
    {
        if (!IsEmpty() && !IsDeleted())
        {
            SetDelete(value); // defend for value cross border of 8Bytes
        }
        mValue = value.Value();
        volatile uint32_t tempTimestamp = value.Timestamp();
        mTimestamp = tempTimestamp;
    }
    void SetDelete(const TimestampValue<_RealVT>& value)
    {
        volatile uint32_t tempTimestamp = value.Timestamp() | DeleteMask;
        mTimestamp = tempTimestamp;
    }
    void SetEmpty()
    {
        mTimestamp = Empty;
        new (&mValue) _RealVT;
    }
    bool operator<(const TimestampValue<_RealVT>& other) const
    {
        if (IsDeleted())
        {
            return other.IsDeleted() ? Timestamp() < other.Timestamp() : true;
        }
        if (other.IsDeleted())
        {
            return false;
        }
        return mValue < other.mValue;
    }

    bool operator==(const TimestampValue<_RealVT>& other) const
    { return mTimestamp == other.mTimestamp && (IsDeleted() || mValue == other.mValue); }

private:
    uint32_t mTimestamp; // max(0x7FFFFFFF-1) is 01/19/2038 11:14:06
    _RealVT mValue;
};
#pragma pack(pop)

//////////////////////////////////////////////////////////////////////
// for no timestamp value, will use special key bucket
template <typename _RealVT>
class Timestamp0Value
{
public:
    Timestamp0Value() : mValue() {}
    explicit Timestamp0Value(uint32_t timestamp, const _RealVT& value = _RealVT())
        : mValue(value)
    { }

public:
    // for User
    typedef _RealVT ValueType;
    uint32_t Timestamp() const { return 0;}
    const _RealVT& Value() const { return mValue; }
    _RealVT& Value() { return mValue; }

public:
    // for SpecialKeyBucket
    bool operator<(const Timestamp0Value<_RealVT>& other) const
    { return mValue < other.mValue; }
    bool operator==(const Timestamp0Value<_RealVT>& other) const
    { return mValue == other.mValue; }

private:
    _RealVT mValue;
};

//////////////////////////////////////////////////////////////////////
// for offset value
template <typename _RealVT,
          _RealVT _EmptyValue = std::numeric_limits<_RealVT>::max(),
          _RealVT _DeleteValue = std::numeric_limits<_RealVT>::max() - 1>
class SpecialValue
{
public:
    SpecialValue() : mValue(_EmptyValue) {}
    SpecialValue(const _RealVT& value) : mValue(value) {}

public:
    // for User
    typedef _RealVT ValueType;
    const _RealVT& Value() const { return mValue; }
    _RealVT& Value() { return mValue; }

public:
    // for SpecialValueBucket
    bool IsEmpty() const { return mValue == _EmptyValue; }
    bool IsDeleted() const { return mValue == _DeleteValue; }
    void SetValue(const SpecialValue<_RealVT, _EmptyValue, _DeleteValue>& value)
    { mValue = value.Value(); }
    void SetDelete(const SpecialValue<_RealVT, _EmptyValue, _DeleteValue>& value)
    { mValue = _DeleteValue; }
    void SetEmpty()
    { mValue = _EmptyValue; }
    bool operator<(const SpecialValue<_RealVT, _EmptyValue, _DeleteValue>& other) const
    { return mValue < other.mValue; }
    bool operator==(const SpecialValue<_RealVT, _EmptyValue, _DeleteValue>& other) const
    { return mValue == other.mValue; }
private:
    _RealVT mValue;
};

#pragma pack(push)
#pragma pack(4)

// for short offset value
template <typename _RealVT,
          _RealVT _EmptyValue = std::numeric_limits<_RealVT>::max(),
          _RealVT _DeleteValue = std::numeric_limits<_RealVT>::max() - 1>
class OffsetValue
{
public:
    OffsetValue() : mValue(_EmptyValue) {}
    OffsetValue(const _RealVT& value) : mValue(value) {}
    explicit OffsetValue(uint32_t timestamp, const _RealVT& value)
        : mValue(value)
    { }

public:
    // for User
    typedef _RealVT ValueType;
    const _RealVT& Value() const { return mValue; }
    _RealVT& Value() { return mValue; }
    uint32_t Timestamp() const { return 0;}    

public:
    // for OffsetValueBucket
    bool IsEmpty() const { return mValue == _EmptyValue; }
    bool IsDeleted() const { return mValue == _DeleteValue; }
    void SetValue(const OffsetValue<_RealVT, _EmptyValue, _DeleteValue>& value)
    { mValue = value.Value(); }
    void SetDelete(const OffsetValue<_RealVT, _EmptyValue, _DeleteValue>& value)
    { mValue = _DeleteValue; }
    void SetEmpty()
    { mValue = _EmptyValue; }
    bool operator<(const OffsetValue<_RealVT, _EmptyValue, _DeleteValue>& other) const
    { return mValue < other.mValue; }
    bool operator==(const OffsetValue<_RealVT, _EmptyValue, _DeleteValue>& other) const
    { return mValue == other.mValue; }
private:
    _RealVT mValue;
};

#pragma pack(pop)

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SPECIAL_VALUE_H
