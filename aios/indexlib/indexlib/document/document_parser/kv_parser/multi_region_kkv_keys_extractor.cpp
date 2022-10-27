#include "indexlib/document/document_parser/kv_parser/multi_region_kkv_keys_extractor.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, MultiRegionKKVKeysExtractor);

MultiRegionKKVKeysExtractor::MultiRegionKKVKeysExtractor(
        const IndexPartitionSchemaPtr& schema)
{
    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++)
    {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(id);
        KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(config::KKVIndexConfig,
                indexSchema->GetPrimaryKeyIndexConfig());
        KKVKeysExtractorPtr extractor(new KKVKeysExtractor(kkvConfig));
        mInnerExtractor.push_back(extractor);
    }
}

bool MultiRegionKKVKeysExtractor::GetKeys(const NormalDocumentPtr& doc,
                                          uint64_t& pkey, uint64_t& skey,
                                          KKVKeysExtractor::OperationType& opType)
{
    regionid_t regionId = doc->GetRegionId();
    if (regionId < 0 || regionId >= (regionid_t)mInnerExtractor.size())
    {
        return false;
    }
    return mInnerExtractor[regionId]->GetKeys(doc, pkey, skey, opType);
}

IE_NAMESPACE_END(document);

