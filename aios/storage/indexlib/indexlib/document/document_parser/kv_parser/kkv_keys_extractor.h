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

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, KKVIndexConfig);
DECLARE_REFERENCE_CLASS(util, KeyHasher);

namespace indexlib { namespace document {

class KKVKeysExtractor
{
public:
    enum OperationType { ADD, DELETE_PKEY, DELETE_SKEY, UNKOWN };

public:
    KKVKeysExtractor(const config::KKVIndexConfigPtr& kkvConfig);
    ~KKVKeysExtractor();

public:
    bool GetKeys(document::KVIndexDocument* doc, uint64_t& pkey, uint64_t& skey, OperationType& operationType);
    bool HashPrefixKey(const std::string& pkey, uint64_t& pkeyHash);
    bool HashSuffixKey(const std::string& suffixKey, uint64_t& skeyHash);

private:
    regionid_t _regionId;
    FieldType _prefixKeyFieldType;
    bool _prefixKeyUseNumberHash;
    FieldType _suffixKeyFieldType;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(KKVKeysExtractor);
}} // namespace indexlib::document
