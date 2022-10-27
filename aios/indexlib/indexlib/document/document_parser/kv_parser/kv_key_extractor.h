#ifndef __INDEXLIB_KV_KEY_EXTRACTOR_H
#define __INDEXLIB_KV_KEY_EXTRACTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/config/kv_index_config.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

IE_NAMESPACE_BEGIN(document);

class KVKeyExtractor
{
public:
    KVKeyExtractor(const config::KVIndexConfigPtr& kvConfig)
        : mRegionId(kvConfig->GetRegionCount() > 1 ? kvConfig->GetRegionId() : INVALID_REGIONID)
    {
        mKeyHasher.reset(util::KeyHasherFactory::Create(
                        kvConfig->GetFieldConfig()->GetFieldType(),
                        kvConfig->UseNumberHash()));
    }
    
    ~KVKeyExtractor() {}
    
public:
    bool GetKey(const document::NormalDocumentPtr& doc, uint64_t& key);
    void HashKey(const std::string& key, uint64_t &keyHash);    
    
private:
    util::KeyHasherPtr mKeyHasher;
    regionid_t mRegionId;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVKeyExtractor);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_KV_KEY_EXTRACTOR_H
