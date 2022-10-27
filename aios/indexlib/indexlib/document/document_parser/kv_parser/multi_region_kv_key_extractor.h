#ifndef __INDEXLIB_MULTI_REGION_KV_KEY_EXTRACTOR_H
#define __INDEXLIB_MULTI_REGION_KV_KEY_EXTRACTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/kv_parser/kv_key_extractor.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(document, NormalDocument);

IE_NAMESPACE_BEGIN(document);

class MultiRegionKVKeyExtractor
{
public:
    MultiRegionKVKeyExtractor(const config::IndexPartitionSchemaPtr& schema);
    ~MultiRegionKVKeyExtractor();
    
public:
    bool GetKey(const document::NormalDocumentPtr& doc, uint64_t& key);
    
    bool HashKey(const std::string& key, uint64_t &keyHash, regionid_t regionId)
    {
        if (regionId < 0 || regionId >= (regionid_t)mInnerExtractor.size())
        {
            IE_LOG(ERROR, "invalid region id [%d]", regionId);
            ERROR_COLLECTOR_LOG(ERROR, "invalid region id [%d]", regionId);
            return false;
        }
        mInnerExtractor[regionId]->HashKey(key, keyHash);
        return true;
    }

private:
    std::vector<KVKeyExtractorPtr> mInnerExtractor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionKVKeyExtractor);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_MULTI_REGION_KV_KEY_EXTRACTOR_H
