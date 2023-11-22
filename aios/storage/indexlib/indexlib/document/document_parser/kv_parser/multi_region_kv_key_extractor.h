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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/kv_parser/kv_key_extractor.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace document {

class MultiRegionKVKeyExtractor
{
public:
    MultiRegionKVKeyExtractor(const config::IndexPartitionSchemaPtr& schema);
    ~MultiRegionKVKeyExtractor();

public:
    bool GetKey(document::KVIndexDocument* doc, uint64_t& key);
    bool HashKey(const std::string& key, uint64_t& keyHash, regionid_t regionId)
    {
        if (regionId < 0 || regionId >= (regionid_t)mInnerExtractor.size()) {
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
}} // namespace indexlib::document
