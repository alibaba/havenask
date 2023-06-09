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
#include "indexlib/index/kkv/collect_info_comparator.h"

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, CollectInfoValueComp);

void CollectInfoValueComp::Init(const KKVIndexConfigPtr& kkvConfig)
{
    mKKVConfig = kkvConfig;
    mFormatter.reset(new PackAttributeFormatter);
    mFormatter->Init(kkvConfig->GetValueConfig()->CreatePackAttributeConfig());
    config::SortParams sortParams = kkvConfig->GetSuffixKeyTruncateParams();
    for (size_t i = 0; i < sortParams.size(); i++) {
        AddSortLayer(sortParams[i]);
    }
    mSkeyComparator = CreateSKeyComparator(mKKVConfig, false);
}

void CollectInfoValueComp::AddSortLayer(const SortParam& sortParam)
{
    const std::string& sortField = sortParam.GetSortField();
    bool isDesc = (sortParam.GetSortPattern() == indexlibv2::config::sp_desc);
    // sortField == ts
    if (sortField == config::KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME) {
        AddTimestampComparator(isDesc);
        return;
    }
    // sortField == skey & skey type is number, use hash value compare
    if (sortField == mKKVConfig->GetSuffixFieldName()) {
        assert(!mKKVConfig->GetSuffixFieldConfig()->IsMultiValue());
        if (mKKVConfig->GetSuffixFieldConfig()->GetFieldType() == ft_string) {
            INDEXLIB_FATAL_ERROR(BadParameter, "sort field [%s] is not sortable!", sortField.c_str());
        }
        mSortComps.push_back(CreateSKeyComparator(mKKVConfig, isDesc));
        return;
    }

    // sortField == value field
    config::AttributeConfigPtr attrConfig = mFormatter->GetAttributeConfig(sortField);
    if (!attrConfig) {
        INDEXLIB_FATAL_ERROR(BadParameter, "sort field [%s] not exist!", sortField.c_str());
    }
    if (attrConfig->IsMultiValue() || attrConfig->GetFieldType() == ft_string) {
        INDEXLIB_FATAL_ERROR(BadParameter, "sort field [%s] is not sortable!", sortField.c_str());
    }
    AttributeReference* ref = mFormatter->GetAttributeReference(sortField);
    if (!ref) {
        INDEXLIB_FATAL_ERROR(BadParameter, "GetAttributeReference for sort field [%s] fail!", sortField.c_str());
    }
    AddSingleValueComparator(ref, isDesc);
}

KKVComparatorPtr CollectInfoValueComp::CreateSKeyComparator(const KKVIndexConfigPtr& kkvConfig, bool isDesc)
{
    assert(kkvConfig);
    FieldType ft = kkvConfig->GetSuffixFieldConfig()->GetFieldType();
    switch (ft) {
#define MACRO(fieldType)                                                                                               \
    case fieldType: {                                                                                                  \
        KKVComparatorPtr comp(new SKeyComparator<typename FieldTypeTraits<fieldType>::AttrItemType>(isDesc));          \
        return comp;                                                                                                   \
    }

        NUMBER_FIELD_MACRO_HELPER(MACRO);
    default: {
        KKVComparatorPtr comp(new SKeyComparator<uint64_t>(isDesc));
        return comp;
    }
#undef MACRO
    }
    return KKVComparatorPtr();
}
}} // namespace indexlib::index
