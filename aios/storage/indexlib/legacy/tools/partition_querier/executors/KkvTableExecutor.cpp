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
#include "indexlib/legacy/tools/partition_querier/executors/KkvTableExecutor.h"

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/config/value_config.h"
#include "indexlib/legacy/tools/partition_querier/executors/AttrHelper.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlibv2::base;

namespace indexlib::tools {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib::tools, KkvTableExecutor);

Status KkvTableExecutor::QueryKkvTable(const IndexPartitionReaderPtr& indexPartitionReaderPtr, regionid_t regionId,
                                       const PartitionQuery& query, PartitionResponse& partitionResponse)
{
    if (query.pk().empty()) {
        return Status::InvalidArgs("pk required for kv table.");
    }
    if ((query.skey_size() != 0) && (query.pk_size() != query.skey_size())) {
        return Status::InvalidArgs("pk count and skey count should be equal in kkv query");
    }
    const auto& kkvReader = indexPartitionReaderPtr->GetKKVReader(regionId);
    if (!kkvReader) {
        return Status::InvalidArgs("failed to create kkv reader.");
    }
    const auto& kkvIndexConfig = kkvReader->GetKKVIndexConfig();
    const auto& valueConfig = kkvIndexConfig->GetValueConfig();
    const auto& packAttrConfig = valueConfig->CreatePackAttributeConfig();
    PackAttributeFormatterPtr formatter(new PackAttributeFormatter);
    if (!formatter->Init(packAttrConfig)) {
        return Status::InvalidArgs("failed to init PackAttributeFormatter.");
    }

    auto [valueAttrs, skFieldName] =
        ValidateAttrs(kkvIndexConfig, packAttrConfig, vector<string> {query.attrs().begin(), query.attrs().end()});
    auto attrs = valueAttrs;
    if (!skFieldName.empty()) {
        attrs.push_back(skFieldName);
    }
    auto skFieldType = kkvIndexConfig->GetSuffixFieldConfig()->GetFieldType();

    const auto& pkList = query.pk();
    const auto& skList = query.skey();
    const int64_t limit = (query.has_limit() && query.limit() != 0) ? query.limit() : 10;
    for (int i = 0; i < pkList.size(); ++i) {
        auto pk = pkList[i];
        uint64_t curTs = 0;
        autil::mem_pool::Pool pool;
        StringView prefixKey = autil::MakeCString(pk, &pool);
        KKVIterator* kkvIterator = nullptr;
        if (skList.empty()) {
            kkvIterator =
                future_lite::interface::syncAwait(kkvReader->LookupAsync(prefixKey, curTs, tsc_default, &pool));
        } else {
            auto sk = skList[i];
            StringView suffixKey = autil::MakeCString(sk, &pool);
            vector<StringView> suffixKeys = {suffixKey};
            kkvIterator = future_lite::interface::syncAwait(
                kkvReader->LookupAsync(prefixKey, suffixKeys, curTs, tsc_default, &pool));
        }
        if (!kkvIterator) {
            return Status::NotFound("record not found for primary key: ", prefixKey);
        }

        FormatKkvResults(kkvIterator, formatter, valueAttrs, skFieldName, skFieldType, limit, partitionResponse);
        POOL_COMPATIBLE_DELETE_CLASS(&pool, kkvIterator);
    }

    AttrHelper::FillAttrInfo(attrs, partitionResponse);

    return Status::OK();
}

void KkvTableExecutor::FormatKkvResults(KKVIterator* iter, const PackAttributeFormatterPtr& formater,
                                        const vector<string>& attrNames, const string& skFieldName,
                                        const FieldType& skFieldType, const int64_t limit,
                                        PartitionResponse& partitionResponse)
{
    while (iter->IsValid() && partitionResponse.rows_size() < limit) {
        StringView value;
        iter->GetCurrentValue(value);
        auto row = partitionResponse.add_rows();
        for (const auto& attr : attrNames) {
            auto attrValue = row->add_attrvalues();
            AttrHelper::GetAttributeValue(value, formater, attr, *attrValue);
        }
        if (iter->IsOptimizeStoreSKey() && !skFieldName.empty()) {
            auto attrValue = row->add_attrvalues();
            uint64_t skey = iter->GetCurrentSkey();
            StringView value((char*)&skey, sizeof(skey));
            AttrHelper::GetKvValue(value, skFieldType, *attrValue);
        }
        iter->MoveToNext();
    }
    iter->Finish();
}

std::tuple<std::vector<std::string>, std::string>
KkvTableExecutor::ValidateAttrs(const KKVIndexConfigPtr& kkvIndexConfig, const PackAttributeConfigPtr& packAttrConfig,
                                const std::vector<std::string>& attrs)
{
    vector<string> kkvAttrs;
    packAttrConfig->GetSubAttributeNames(kkvAttrs);
    vector<string> allAttrs = kkvAttrs;
    string skFieldName = kkvIndexConfig->GetSuffixFieldConfig()->GetFieldName();
    if (kkvIndexConfig->OptimizedStoreSKey()) {
        allAttrs.push_back(skFieldName);
    }
    auto outAttrs = AttrHelper::ValidateAttrs(allAttrs, attrs);

    if (kkvIndexConfig->OptimizedStoreSKey()) {
        auto it = find(outAttrs.begin(), outAttrs.end(), skFieldName);
        if (it != outAttrs.end()) {
            outAttrs.erase(it);
        } else {
            skFieldName = "";
        }
    } else {
        skFieldName = "";
    }
    return make_tuple(outAttrs, skFieldName);
}

} // namespace indexlib::tools
