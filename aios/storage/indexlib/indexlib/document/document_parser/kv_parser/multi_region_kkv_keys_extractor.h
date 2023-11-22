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
#include "indexlib/document/document_parser/kv_parser/kkv_keys_extractor.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
namespace indexlib { namespace document {

class MultiRegionKKVKeysExtractor
{
public:
    MultiRegionKKVKeysExtractor(const config::IndexPartitionSchemaPtr& schema);
    ~MultiRegionKKVKeysExtractor() {}

public:
    bool HashPrefixKey(const std::string& pkey, uint64_t& pkeyHash, regionid_t regionId)
    {
        if (regionId < 0 || regionId >= (regionid_t)mInnerExtractor.size()) {
            return false;
        }
        return mInnerExtractor[regionId]->HashPrefixKey(pkey, pkeyHash);
    }

    bool HashSuffixKey(const std::string& suffixKey, uint64_t& skeyHash, regionid_t regionId)
    {
        if (regionId < 0 || regionId >= (regionid_t)mInnerExtractor.size()) {
            return false;
        }
        return mInnerExtractor[regionId]->HashSuffixKey(suffixKey, skeyHash);
    }

    bool GetKeys(document::KVIndexDocument* doc, uint64_t& pkey, uint64_t& skey,
                 KKVKeysExtractor::OperationType& opType);

private:
    std::vector<KKVKeysExtractorPtr> mInnerExtractor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionKKVKeysExtractor);
}} // namespace indexlib::document
