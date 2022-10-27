#ifndef __INDEXLIB_MULTI_REGION_SPECIAL_VALUE_H
#define __INDEXLIB_MULTI_REGION_SPECIAL_VALUE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

struct MultiRegionOffsetFormatter
{
private:
    struct _InnerStruct
    {
        uint64_t offset : 50;
        uint64_t regionId : 12;
        uint64_t flag : 2;
    };
    union _InnerOffsetUnion
    {
        uint64_t value;
        _InnerStruct innerStruct;
    };
public:
    MultiRegionOffsetFormatter()
    {
        SetEmpty();
    }

    MultiRegionOffsetFormatter(uint64_t value)
    {
        mUnionValue.value = value;
    }
    
    MultiRegionOffsetFormatter(uint64_t offset, regionid_t regionId)
    {
        SetOffset(offset, regionId);
    }
public:
    uint64_t GetValue() const { return mUnionValue.value; }
    
    void SetEmpty()
    {
        mUnionValue.value = 0;
        assert(IsEmpty());
    }
    
    void SetDeleted(regionid_t regionId)
    {
        mUnionValue.innerStruct.regionId = regionId;
        mUnionValue.innerStruct.flag = 1;
    }

    void SetOffset(uint64_t offset, regionid_t regionId)
    {
        mUnionValue.innerStruct.offset = offset;
        mUnionValue.innerStruct.regionId = regionId;
        mUnionValue.innerStruct.flag = 2;
    }

    bool IsEmpty() const { return mUnionValue.innerStruct.flag == 0; }
    bool IsDeleted() const { return mUnionValue.innerStruct.flag == 1; }
    regionid_t GetRegionId() const { return mUnionValue.innerStruct.regionId; }
    uint64_t GetOffset() const { return mUnionValue.innerStruct.offset; }

    bool operator == (const MultiRegionOffsetFormatter& other) const
    {
        return GetRegionId() == other.GetRegionId()
            && IsEmpty() == other.IsEmpty()
            && IsDeleted() == other.IsDeleted()
            && (IsDeleted() || GetOffset() == other.GetOffset());
    }
    
private:
    _InnerOffsetUnion mUnionValue;
}; 

// for timestamp value
#pragma pack(push)
#pragma pack(4)
template <typename _RealVT>
class MultiRegionTimestampValue
{
public:
    MultiRegionTimestampValue() : mTimestamp(0), mValue()
    {
        MultiRegionOffsetFormatter formatter;
        mValue = formatter.GetValue();
        assert(IsEmpty());
        assert(!IsDeleted());
    }
    explicit MultiRegionTimestampValue(uint32_t timestamp)
        : mTimestamp(timestamp)
        , mValue()
    {
        MultiRegionOffsetFormatter formatter;
        formatter.SetDeleted(DEFAULT_REGIONID);
        mValue = formatter.GetValue();
        assert(!IsEmpty());
        assert(IsDeleted());
        assert(Timestamp() == timestamp);
    }

    explicit MultiRegionTimestampValue(uint32_t timestamp, const _RealVT& value)
        : mTimestamp(timestamp)
        , mValue(value)
    {
        assert(!IsEmpty());
        assert(!IsDeleted());
        assert(Timestamp() == timestamp);
    }

public:
    // for User
    typedef _RealVT ValueType;
    uint32_t Timestamp() const { return mTimestamp; }
    const _RealVT& Value() const { return mValue; }
    _RealVT& Value() { return mValue; }

public:
    // for SpecialValueBucket
    bool IsEmpty() const { return MultiRegionOffsetFormatter(mValue).IsEmpty(); }
    bool IsDeleted() const { return MultiRegionOffsetFormatter(mValue).IsDeleted(); }
    void SetValue(const MultiRegionTimestampValue<_RealVT>& value)
    {
        if (!IsEmpty() && !IsDeleted())
        {
            SetDelete(value); // defend for value cross border of 8Bytes
        }
        mValue = value.Value();
        volatile uint32_t tempTimestamp = value.Timestamp();
        mTimestamp = tempTimestamp;
    }
    void SetDelete(const MultiRegionTimestampValue<_RealVT>& value)
    {
        volatile uint32_t tempTimestamp = value.Timestamp();
        mTimestamp = tempTimestamp;

        MultiRegionOffsetFormatter formatter;
        formatter.SetDeleted(MultiRegionOffsetFormatter(value.Value()).GetRegionId());
        mValue = formatter.GetValue();
    }
    void SetEmpty()
    {
        MultiRegionOffsetFormatter formatter(mValue);
        formatter.SetEmpty();
        mValue = formatter.GetValue();
    }
    bool operator<(const MultiRegionTimestampValue<_RealVT>& other) const
    {
        if (IsDeleted())
        {
            return other.IsDeleted() ? Timestamp() < other.Timestamp() : true;
        }
        if (other.IsDeleted())
        {
            return false;
        }
        return MultiRegionOffsetFormatter(mValue).GetOffset() <
            MultiRegionOffsetFormatter(other.mValue).GetOffset();
    }

    bool operator==(const MultiRegionTimestampValue<_RealVT>& other) const
    {
        if (mTimestamp != other.mTimestamp)
        {
            return false;
        }
        MultiRegionOffsetFormatter formatter(mValue);
        MultiRegionOffsetFormatter otherFormatter(other.mValue);
        return formatter == otherFormatter;
    }

private:
    uint32_t mTimestamp;
    _RealVT mValue;
};
#pragma pack(pop)

//////////////////////////////////////////////////////////////////////
// for no timestamp value, will use special value bucket
template <typename _RealVT>
class MultiRegionTimestamp0Value
{
public:
    MultiRegionTimestamp0Value() : mValue()
    {
        MultiRegionOffsetFormatter formatter;
        mValue = formatter.GetValue();
        assert(IsEmpty());
        assert(!IsDeleted());
    }

    explicit MultiRegionTimestamp0Value(uint32_t timestamp)
        : mValue()
    {
        MultiRegionOffsetFormatter formatter;
        formatter.SetDeleted(DEFAULT_REGIONID);
        mValue = formatter.GetValue();
        assert(!IsEmpty());
        assert(IsDeleted());
    }

    explicit MultiRegionTimestamp0Value(uint32_t timestamp, const _RealVT& value)
        : mValue(value)
    {
        assert(!IsEmpty());
        assert(!IsDeleted());
    }

public:
    // for User
    typedef _RealVT ValueType;
    uint32_t Timestamp() const { return 0; }
    const _RealVT& Value() const { return mValue; }
    _RealVT& Value() { return mValue; }

public:
    // for SpecialValueBucket
    bool IsEmpty() const { return MultiRegionOffsetFormatter(mValue).IsEmpty(); }
    bool IsDeleted() const { return MultiRegionOffsetFormatter(mValue).IsDeleted(); }
    void SetValue(const MultiRegionTimestamp0Value<_RealVT>& value)
    {
        mValue = value.Value();
    }
    void SetDelete(const MultiRegionTimestamp0Value<_RealVT>& value)
    {
        MultiRegionOffsetFormatter formatter;
        formatter.SetDeleted(MultiRegionOffsetFormatter(value.Value()).GetRegionId());
        mValue = formatter.GetValue();
    }
    void SetEmpty()
    {
        MultiRegionOffsetFormatter formatter(mValue);
        formatter.SetEmpty();
        mValue = formatter.GetValue();
    }
    bool operator<(const MultiRegionTimestamp0Value<_RealVT>& other) const
    {
        if (IsDeleted() && !other.IsDeleted())
        {
            return true;
        }
        if (other.IsDeleted())
        {
            return false;
        }
        return MultiRegionOffsetFormatter(mValue).GetOffset() <
            MultiRegionOffsetFormatter(other.mValue).GetOffset();
    }

    bool operator==(const MultiRegionTimestamp0Value<_RealVT>& other) const
    {
        MultiRegionOffsetFormatter formatter(mValue);
        MultiRegionOffsetFormatter otherFormatter(other.mValue);
        return formatter == otherFormatter;
    }

private:
    _RealVT mValue;
};

IE_NAMESPACE_END(index);

IE_NAMESPACE_BEGIN(common);

// multi region use special value
template<typename _KT, typename _RealVT>
struct ClosedHashTableTraits<_KT, index::MultiRegionTimestampValue<_RealVT>, 
                             false>
{
    static const bool HasSpecialKey = false;
    typedef index::MultiRegionTimestampValue<_RealVT> _VT;
    typedef SpecialValueBucket<_KT, _VT> Bucket;
};

template<typename _KT, typename _RealVT>
struct ClosedHashTableTraits<_KT, index::MultiRegionTimestamp0Value<_RealVT>,
                             false>
{
    static const bool HasSpecialKey = false;
    typedef index::MultiRegionTimestamp0Value<_RealVT> _VT;
    typedef SpecialValueBucket<_KT, _VT> Bucket;
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_MULTI_REGION_SPECIAL_VALUE_H
