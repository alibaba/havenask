#ifndef __INDEXLIB_KKV_KEYS_EXTRACTOR_H
#define __INDEXLIB_KKV_KEYS_EXTRACTOR_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, KKVIndexConfig);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(util, KeyHasher);

IE_NAMESPACE_BEGIN(document);

class KKVKeysExtractor
{
public:
    enum OperationType
    {
        ADD,
        DELETE_PKEY,
        DELETE_SKEY,
        UNKOWN
    };

public:
    KKVKeysExtractor(const config::KKVIndexConfigPtr& kkvConfig);
    ~KKVKeysExtractor();
    
public:
    bool GetKeys(const document::NormalDocumentPtr& doc,
                 uint64_t& pkey, uint64_t& skey, OperationType& opType);
    bool HashPrefixKey(const std::string& pkey, uint64_t &pkeyHash);
    bool HashSuffixKey(const std::string& suffixKey, uint64_t &skeyHash);

private:
    bool AddKVIndexDocForLegacy(const document::NormalDocumentPtr& doc);

private:
    util::KeyHasherPtr mPrefixKeyHasher; 
    util::KeyHasherPtr mSuffixKeyHasher;
    regionid_t mRegionId;
private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(KKVKeysExtractor);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_KKV_KEYS_EXTRACTOR_H
