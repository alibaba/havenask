#ifndef __INDEXLIB_CLOSED_HASH_TABLE_TRAITS_H
#define __INDEXLIB_CLOSED_HASH_TABLE_TRAITS_H

#include "indexlib/common/hash_table/special_value.h"
#include "indexlib/common/hash_table/special_key_bucket.h"
#include "indexlib/common/hash_table/compact_special_key_bucket.h"
#include "indexlib/common/hash_table/special_value_bucket.h"

IE_NAMESPACE_BEGIN(common);

// for normal
template<typename _KT, typename _VT, bool useCompactBucket>
struct ClosedHashTableTraits
{
    static const bool HasSpecialKey = true;
    static const bool UseCompactBucket = false;
    typedef SpecialKeyBucket<_KT, _VT> Bucket;
    typedef typename Bucket::SpecialBucket SpecialBucket;
};

template<typename _KT, typename _VT>
struct ClosedHashTableTraits<_KT, _VT, true>
{
    static const bool HasSpecialKey = true;
    static const bool UseCompactBucket = true;
    typedef CompactSpecialKeyBucket<_KT, _VT> Bucket;
    typedef typename Bucket::SpecialBucket SpecialBucket;
};

// for timestamp value
template<typename _KT, typename _RealVT>
struct ClosedHashTableTraits<_KT, TimestampValue<_RealVT>, false>
{
    static const bool HasSpecialKey = false;
    static const bool UseCompactBucket = false;
    typedef TimestampValue<_RealVT> _VT;
    typedef SpecialValueBucket<_KT, _VT> Bucket;
};

// for offset value
template<typename _KT, typename _RealVT,
         _RealVT EmptyValue, _RealVT DeleteValue>
struct ClosedHashTableTraits<_KT, SpecialValue<_RealVT, EmptyValue, DeleteValue>, 
                             false>
{
    static_assert(sizeof(_RealVT) == 4 || sizeof(_RealVT) == 8,
                  "to guarantee concurrent read and write");

    static const bool HasSpecialKey = false;
    static const bool UseCompactBucket = false;
    typedef SpecialValue<_RealVT, EmptyValue, DeleteValue> _VT;
    typedef SpecialValueBucket<_KT, _VT> Bucket;
};

// for short offset value
template<typename _KT, typename _RealVT>
struct ClosedHashTableTraits<_KT, OffsetValue<_RealVT>, false>
{
    static_assert(sizeof(_RealVT) == 4 || sizeof(_RealVT) == 8,
                  "to guarantee concurrent read and write");

    static const bool HasSpecialKey = false;
    static const bool UseCompactBucket = false;
    typedef OffsetValue<_RealVT> _VT;
    typedef SpecialValueBucket<_KT, _VT> Bucket;
};


IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CLOSED_HASH_TABLE_TRAITS_H
