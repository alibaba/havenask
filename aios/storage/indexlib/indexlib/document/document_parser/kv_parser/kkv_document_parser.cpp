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
#include "indexlib/document/document_parser/kv_parser/kkv_document_parser.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kkv_keys_extractor.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, KkvDocumentParser);

KkvDocumentParser::KkvDocumentParser(const IndexPartitionSchemaPtr& schema)
    : KvDocumentParser(schema)
    , mDenyEmptySuffixKey(false)
{
}

KkvDocumentParser::~KkvDocumentParser() {}

bool KkvDocumentParser::DoInit()
{
    const auto& userDefinedParam = mSchema->GetUserDefinedParam();
    auto itr = userDefinedParam.find("deny_empty_skey");
    if (userDefinedParam.end() != itr) {
        mDenyEmptySuffixKey = autil::legacy::AnyCast<bool>(itr->second);
        IE_LOG(INFO, "deny_empty_skey is [%s]", mDenyEmptySuffixKey ? "true" : "false");
    }
    return KvDocumentParser::DoInit();
}

bool KkvDocumentParser::InitKeyExtractor()
{
    if (mSchema->GetTableType() != tt_kkv) {
        ERROR_COLLECTOR_LOG(ERROR,
                            "init KkvDocumentParser fail, "
                            "unsupport tableType [%s]!",
                            IndexPartitionSchema::TableType2Str(mSchema->GetTableType()).c_str());
        return false;
    }

    mKkvKeyExtractor.reset(new MultiRegionKKVKeysExtractor(mSchema));
    return true;
}

bool KkvDocumentParser::SetPrimaryKeyField(const IndexlibExtendDocumentPtr& document, const IndexSchemaPtr& indexSchema,
                                           regionid_t regionId, DocOperateType opType,
                                           document::KVIndexDocument* kvIndexDoc)
{
    const string& pkFieldName = indexSchema->GetPrimaryKeyIndexFieldName();
    if (pkFieldName.empty()) {
        ERROR_COLLECTOR_LOG(ERROR, "prefix key field is empty!");
        return true;
    }

    const RawDocumentPtr& rawDoc = document->GetRawDocument();

    string pkValue = rawDoc->getField(pkFieldName);
    if (mDenyEmptyPrimaryKey && pkValue.empty()) {
        ERROR_COLLECTOR_LOG(ERROR, "prefix key value is empty!");
        return false;
    }

    uint64_t pkeyHash = 0;
    mKkvKeyExtractor->HashPrefixKey(pkValue, pkeyHash, regionId);

    KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    assert(kkvIndexConfig);
    const string& skeyFieldName = kkvIndexConfig->GetSuffixFieldName();

    string skeyValue = rawDoc->getField(skeyFieldName);
    if (mDenyEmptySuffixKey && skeyValue.empty()) {
        // delete message without skey means delete all skeys in the pkey
        if (rawDoc->getDocOperateType() != DELETE_DOC) {
            ERROR_COLLECTOR_LOG(ERROR, "suffix key value is empty!");
            return false;
        }
    }

    uint64_t skeyHash = 0;
    bool hasSkey = false;
    if (!skeyValue.empty()) {
        mKkvKeyExtractor->HashSuffixKey(skeyValue, skeyHash, regionId);
        hasSkey = true;
    }

    if (!hasSkey && opType == ADD_DOC) {
        ERROR_COLLECTOR_LOG(ERROR,
                            "suffix key field is empty for add doc, "
                            "which prefix key is [%s]!",
                            pkValue.c_str());
        return true;
    }

    if (hasSkey) {
        document->setIdentifier(pkValue + ":" + skeyValue);
    } else {
        document->setIdentifier(pkValue);
    }
    kvIndexDoc->SetPKeyHash(pkeyHash);
    if (hasSkey) {
        kvIndexDoc->SetSKeyHash(skeyHash);
    }

    return true;
}
}} // namespace indexlib::document
