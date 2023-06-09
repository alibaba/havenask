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
#include "indexlib/document/document_parser/kv_parser/kv_key_extractor.h"

#include "autil/ConstString.h"
#include "indexlib/common/multi_region_rehasher.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/index/common/KeyHasherWrapper.h"

using namespace indexlib::common;
using namespace indexlib::document;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, KVKeyExtractor);

bool KVKeyExtractor::GetKey(document::KVIndexDocument* doc, uint64_t& key)
{
    if (_regionId != INVALID_REGIONID && _regionId != doc->GetRegionId()) {
        IE_LOG(ERROR, "region id miss match between doc [%d] and schema [%d]", doc->GetRegionId(), _regionId);
        ERROR_COLLECTOR_LOG(ERROR, "region id miss match between doc [%d] and schema [%d]", doc->GetRegionId(),
                            _regionId);
        return false;
    }
    key = doc->GetPKeyHash();
    if (_regionId != INVALID_REGIONID) {
        key = MultiRegionRehasher::GetRehashKey(key, _regionId);
    }
    return true;
}

void KVKeyExtractor::HashKey(const std::string& key, uint64_t& keyHash)
{
    autil::StringView keyStr(key);
    bool ret = index::KeyHasherWrapper::GetHashKey(_fieldType, _useNumberHash, keyStr.data(), keyStr.size(), keyHash);
    assert(ret);
    (void)ret;
}
}} // namespace indexlib::document
