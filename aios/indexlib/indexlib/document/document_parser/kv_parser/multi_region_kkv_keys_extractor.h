#ifndef __INDEXLIB_MULTI_REGION_KKV_KEYS_EXTRACTOR_H
#define __INDEXLIB_MULTI_REGION_KKV_KEYS_EXTRACTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/kv_parser/kkv_keys_extractor.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
IE_NAMESPACE_BEGIN(document);

class MultiRegionKKVKeysExtractor
{
public:
    MultiRegionKKVKeysExtractor(const config::IndexPartitionSchemaPtr& schema);
    ~MultiRegionKKVKeysExtractor() {}
    
public:
    bool HashPrefixKey(const std::string& pkey, uint64_t &pkeyHash,
                       regionid_t regionId)
    {
        if (regionId < 0 || regionId >= (regionid_t)mInnerExtractor.size())
        {
            return false;
        }
        return mInnerExtractor[regionId]->HashPrefixKey(pkey, pkeyHash);
    }
    
    bool HashSuffixKey(const std::string& suffixKey, uint64_t &skeyHash,
                       regionid_t regionId)
    {
        if (regionId < 0 || regionId >= (regionid_t)mInnerExtractor.size())
        {
            return false;
        }
        return mInnerExtractor[regionId]->HashSuffixKey(suffixKey, skeyHash);
    }

    bool GetKeys(const document::NormalDocumentPtr& doc,
                 uint64_t& pkey, uint64_t& skey,
                 KKVKeysExtractor::OperationType& opType);
private:
    std::vector<KKVKeysExtractorPtr> mInnerExtractor;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionKKVKeysExtractor);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_MULTI_REGION_KKV_KEYS_EXTRACTOR_H
