/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/document/document_parser/kv_parser/multi_region_kkv_keys_extractor.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, MultiRegionKKVKeysExtractor);

MultiRegionKKVKeysExtractor::MultiRegionKKVKeysExtractor(const IndexPartitionSchemaPtr& schema)
{
    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(id);
        KKVIndexConfigPtr kkvConfig =
            DYNAMIC_POINTER_CAST(config::KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        KKVKeysExtractorPtr extractor(new KKVKeysExtractor(kkvConfig));
        mInnerExtractor.push_back(extractor);
    }
}

bool MultiRegionKKVKeysExtractor::GetKeys(document::KVIndexDocument* doc, uint64_t& pkey, uint64_t& skey,
                                          KKVKeysExtractor::OperationType& opType)
{
    regionid_t regionId = doc->GetRegionId();
    if (regionId < 0 || regionId >= (regionid_t)mInnerExtractor.size()) {
        return false;
    }
    return mInnerExtractor[regionId]->GetKeys(doc, pkey, skey, opType);
}
}} // namespace indexlib::document
