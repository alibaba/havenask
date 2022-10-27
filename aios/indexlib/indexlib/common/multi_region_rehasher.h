#ifndef __INDEXLIB_MULTI_REGION_REHASHER_H
#define __INDEXLIB_MULTI_REGION_REHASHER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_hasher_typed.h"

IE_NAMESPACE_BEGIN(common);

class MultiRegionRehasher
{
public:
    MultiRegionRehasher() {}
    ~MultiRegionRehasher() {}
    
public:
    static inline dictkey_t GetRehashKey(dictkey_t keyHash, regionid_t regionId)
    {
        autil::ConstString rehashKeyStr((char*)&keyHash, sizeof(dictkey_t));
        dictkey_t rehashKey = 0;
        util::MurmurHasher::DoGetHashKey(rehashKeyStr, rehashKey, regionId);
        return rehashKey;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionRehasher);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_MULTI_REGION_REHASHER_H
