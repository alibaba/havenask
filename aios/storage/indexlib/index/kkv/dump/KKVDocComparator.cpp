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
#include "indexlib/index/kkv/dump/KKVDocComparator.h"

#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kkv/common/Trait.h"

using namespace std;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, KKVDocComparator);

void KKVDocComparator::Init(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& kkvIndexConfig)
{
    _kKVConfig = kkvIndexConfig;
    _formatter.reset(new PackAttributeFormatter);
    auto [status, packAttrConfig] = kkvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
    // TODO(chekong.ygm): check status
    _formatter->Init(packAttrConfig);
    const auto& sortParams = kkvIndexConfig->GetSuffixKeyTruncateParams();
    for (size_t i = 0; i < sortParams.size(); i++) {
        AddSortLayer(sortParams[i]);
    }
    _sKeyFieldComparator = CreateSKeyFieldComparator(_kKVConfig, false);
}

void KKVDocComparator::AddSortLayer(const indexlib::config::SortParam& sortParam)
{
    const std::string& sortField = sortParam.GetSortField();
    bool isDesc = (sortParam.GetSortPattern() == indexlibv2::config::sp_desc);
    // sortField == ts
    if (sortField == config::KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME) {
        AddTimestampComparator(isDesc);
        return;
    }
    // sortField == skey & skey type is number, use hash value compare
    if (sortField == _kKVConfig->GetSuffixFieldName()) {
        assert(!_kKVConfig->GetSuffixFieldConfig()->IsMultiValue());
        if (_kKVConfig->GetSuffixFieldConfig()->GetFieldType() == ft_string) {
            INDEXLIB_FATAL_ERROR(BadParameter, "sort field [%s] is not sortable!", sortField.c_str());
        }
        _sortComps.push_back(CreateSKeyFieldComparator(_kKVConfig, isDesc));
        return;
    }

    // sortField == value field
    auto attrConfig = _formatter->GetAttributeConfig(sortField);
    if (!attrConfig) {
        INDEXLIB_FATAL_ERROR(BadParameter, "sort field [%s] not exist!", sortField.c_str());
    }
    if (attrConfig->IsMultiValue() || attrConfig->GetFieldType() == ft_string) {
        INDEXLIB_FATAL_ERROR(BadParameter, "sort field [%s] is not sortable!", sortField.c_str());
    }
    auto ref = _formatter->GetAttributeReference(sortField);
    if (!ref) {
        INDEXLIB_FATAL_ERROR(BadParameter, "GetAttributeReference for sort field [%s] fail!", sortField.c_str());
    }
    AddSingleValueComparator(ref, isDesc);
}

std::shared_ptr<KKVDocFieldComparator>
KKVDocComparator::CreateSKeyFieldComparator(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& kkvIndexConfig,
                                            bool isDesc)
{
    assert(kkvIndexConfig);
    FieldType skeyDictType = kkvIndexConfig->GetSKeyDictKeyType();
    switch (skeyDictType) {
#define CASE_MACRO(fieldType)                                                                                          \
    case fieldType: {                                                                                                  \
        using SkeyType = typename indexlib::index::FieldTypeTraits<fieldType>::AttrItemType;                           \
        return std::make_shared<SKeyFieldComparator<SkeyType>>(isDesc);                                                \
    }
        KKV_SKEY_DICT_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        AUTIL_LOG(ERROR, "unsupported skey field_type:[%s]", config::FieldConfig::FieldTypeToStr(skeyDictType));
        return nullptr;
    }
    }
}

} // namespace indexlibv2::index
