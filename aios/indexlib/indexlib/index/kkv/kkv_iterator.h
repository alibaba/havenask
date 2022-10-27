#ifndef __INDEXLIB_KKV_ITERATOR_H
#define __INDEXLIB_KKV_ITERATOR_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"

IE_NAMESPACE_BEGIN(index);

class KKVIteratorImplBase;

class KKVIterator
{
public:
    KKVIterator(autil::mem_pool::Pool* pool, KKVIteratorImplBase* iter,
                FieldType skeyDictFieldType, bool useCache,
                config::KKVIndexConfig* kkvConfig)
    {}

    ~KKVIterator() {}
public:
    bool IsValid() const { assert(false); return false; }
    void MoveToNext() { assert(false); }
    void GetCurrentValue(autil::ConstString& value) { assert(false); }
    uint64_t GetCurrentTimestamp() { assert(false); return 0; }
    uint64_t GetCurrentSkey() { assert(false); return 0; }

    // if skey value is same with skey hash value, skey will not store in value
    bool IsOptimizeStoreSKey() const { assert(false); return false; }
    const std::string& GetSKeyFieldName() const { assert(false); return ""; }
    void Finish() { assert(false); }
    bool IsSorted() const { assert(false); return false; }
    regionid_t GetRegionId() const { assert(false); return 0; }
};

DEFINE_SHARED_PTR(KKVIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_KKV_ITERATOR_H
