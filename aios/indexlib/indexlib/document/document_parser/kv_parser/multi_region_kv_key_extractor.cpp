#include "indexlib/document/document_parser/kv_parser/multi_region_kv_key_extractor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, MultiRegionKVKeyExtractor);

MultiRegionKVKeyExtractor::MultiRegionKVKeyExtractor(
        const IndexPartitionSchemaPtr& schema)
{
    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++)
    {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(id);
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(config::KVIndexConfig,
                indexSchema->GetPrimaryKeyIndexConfig());
        KVKeyExtractorPtr extractor(new KVKeyExtractor(kvConfig));
        mInnerExtractor.push_back(extractor);
    }
}

MultiRegionKVKeyExtractor::~MultiRegionKVKeyExtractor() 
{
}

bool MultiRegionKVKeyExtractor::GetKey(const NormalDocumentPtr& doc, uint64_t& key)
{
    regionid_t regionId = doc->GetRegionId();
    if (regionId < 0 || regionId >= (regionid_t)mInnerExtractor.size())
    {
        IE_LOG(ERROR, "invalid region id [%d]", regionId);
        ERROR_COLLECTOR_LOG(ERROR, "invalid region id [%d]", regionId);
        return false;
    }
    return mInnerExtractor[regionId]->GetKey(doc, key);
}

IE_NAMESPACE_END(document);

