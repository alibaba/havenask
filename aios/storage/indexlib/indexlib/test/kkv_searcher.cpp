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
#include "indexlib/test/kkv_searcher.h"

#include <memory>

#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/common/field_format/pack_attribute//pack_attribute_formatter.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/value_config.h"
#include "indexlib/index/kkv/kkv_coro_define.h"
#include "indexlib/index/kkv/kkv_reader.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/test/document_parser.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::partition;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, KKVSearcher);

KKVSearcher::KKVSearcher() {}

KKVSearcher::~KKVSearcher() {}

void KKVSearcher::Init(const IndexPartitionReaderPtr& reader, IndexPartitionSchema* schema, const string& regionName)
{
    assert(reader);
    assert(schema);
    regionid_t regionId = schema->GetRegionId(regionName);
    mReader = reader->GetKKVReader(regionId);
    assert(mReader);

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(regionId);
    assert(indexSchema);
    KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    assert(kkvIndexConfig);
    mValueConfig = kkvIndexConfig->GetValueConfig();
    assert(mValueConfig);
    mPackAttrFormatter.reset(new PackAttributeFormatter);
    if (!mPackAttrFormatter->Init(mValueConfig->CreatePackAttributeConfig())) {
        assert(false);
    }
    auto primaryKeyConfig = schema->GetRegionSchema(regionName)->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    mKKVConfig = DYNAMIC_POINTER_CAST(config::KKVIndexConfig, primaryKeyConfig);
}

ResultPtr KKVSearcher::Search(const string& prefixKey, uint64_t timestamp, TableSearchCacheType searchCacheType)
{
    StringView pKey(prefixKey);
    auto queryTask = [&, this]() -> FL_LAZY(ResultPtr) {
        auto kkvDocIter = FL_COAWAIT mReader->LookupAsync(pKey, timestamp, searchCacheType, &mPool, nullptr);
        ResultPtr result(new test::Result);
        while (kkvDocIter && kkvDocIter->IsValid()) {
            FillResult(result, kkvDocIter);
            kkvDocIter->MoveToNext();
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvDocIter);
        FL_CORETURN result;
    };
    future_lite::executors::SimpleExecutor ex(1);
    return future_lite::interface::syncAwait(queryTask(), &ex);
}

ResultPtr KKVSearcher::Search(const string& prefixKey, const vector<string>& suffixKeys, uint64_t timestamp,
                              TableSearchCacheType searchCacheType)
{
    StringView pKey(prefixKey);
    vector<StringView> sKeys;
    for (size_t i = 0; i < suffixKeys.size(); ++i) {
        sKeys.push_back(StringView(suffixKeys[i]));
    }

    auto queryTask = [&, this]() -> FL_LAZY(ResultPtr) {
        auto kkvDocIter = FL_COAWAIT mReader->LookupAsync(pKey, sKeys, timestamp, searchCacheType, &mPool, nullptr);
        ResultPtr result(new test::Result);
        while (kkvDocIter && kkvDocIter->IsValid()) {
            FillResult(result, kkvDocIter);
            kkvDocIter->MoveToNext();
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvDocIter);
        FL_CORETURN result;
    };
    future_lite::executors::SimpleExecutor ex(1);
    return future_lite::interface::syncAwait(queryTask(), &ex);
}

void KKVSearcher::FillResult(const ResultPtr& result, KKVIterator* iter)
{
    StringView value;
    iter->GetCurrentValue(value);
    uint64_t timestamp = iter->GetCurrentTimestamp();

    RawDocumentPtr rawDoc(new RawDocument);
    vector<string> subAttrs;
    PackAttributeConfigPtr packAttrConfig = mValueConfig->CreatePackAttributeConfig();
    assert(packAttrConfig);

    const std::vector<AttributeConfigPtr>& subAttrConfigVec = packAttrConfig->GetAttributeConfigVec();
    for (const auto& subAttr : subAttrConfigVec) {
        const string& attrName = subAttr->GetAttrName();
        AttributeReference* attrRef = mPackAttrFormatter->GetAttributeReference(attrName);
        if (!attrRef) {
            continue;
        }
        string fieldValue;
        attrRef->GetStrValue(value.data(), fieldValue, &mPool);
        if (subAttr->IsMultiValue()) {
            fieldValue = DocumentParser::ParseMultiValueResult(fieldValue);
        }
        rawDoc->SetField(attrName, fieldValue);
    }

    if (iter->IsOptimizeStoreSKey()) {
        rawDoc->SetField(iter->GetSKeyFieldName(), iter->TEST_GetSKeyValueStr());
    }
    rawDoc->SetField(RESERVED_TIMESTAMP, std::to_string(timestamp));
    result->AddDoc(rawDoc);
}

std::string KKVSearcher::GetSKeyValueStr(uint64_t skey)
{
    FieldType fieldType = mKKVConfig->GetSuffixFieldConfig()->GetFieldType();
    switch (fieldType) {
#define MACRO(type)                                                                                                    \
    case type:                                                                                                         \
        return autil::StringUtil::toString<config::FieldTypeTraits<type>::AttrItemType>(skey);

        NUMBER_FIELD_MACRO_HELPER(MACRO)
#undef MACRO
    default:
        assert(false);
    }
    return std::string("");
}
}} // namespace indexlib::test
