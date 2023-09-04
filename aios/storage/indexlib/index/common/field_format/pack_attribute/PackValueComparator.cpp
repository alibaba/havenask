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
#include "indexlib/index/common/field_format/pack_attribute/PackValueComparator.h"

#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PackValueComparator);

PackValueComparator::PackValueComparator() = default;

PackValueComparator::~PackValueComparator() = default;

bool PackValueComparator::Init(const std::shared_ptr<config::ValueConfig>& valueConfig,
                               const config::SortDescriptions& sortDescriptions, bool needDecode)
{
    _fixedValueLen = valueConfig->GetFixedLength();
    _needDecode = needDecode;
    _formatter = std::make_unique<PackAttributeFormatter>();
    auto [status, packAttributeConfig] = valueConfig->CreatePackAttributeConfig();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create pack attribute config failed");
        return false;
    }
    if (!_formatter->Init(packAttributeConfig)) {
        AUTIL_LOG(ERROR, "init attibute formatter failed!");
        return false;
    }

    for (const auto& sortDescription : sortDescriptions) {
        const auto& attributeConfig = _formatter->GetAttributeConfig(sortDescription.GetSortFieldName());
        if (!attributeConfig) {
            AUTIL_LOG(ERROR, "get attribute config for sort field [%s] failed!",
                      sortDescription.GetSortFieldName().c_str());
            return false;
        }

        if (attributeConfig->IsMultiValue() || ft_string == attributeConfig->GetFieldType()) {
            AUTIL_LOG(ERROR, "cannot support multi field type or string type for sort field [%s]!",
                      sortDescription.GetSortFieldName().c_str());
            return false;
        }

        auto ref = _formatter->GetAttributeReference(sortDescription.GetSortFieldName());
        if (!ref) {
            AUTIL_LOG(ERROR, "get attribute reference for sort field [%s] failed!",
                      sortDescription.GetSortFieldName().c_str());
            return false;
        }

        auto attributeComparator =
            std::make_unique<AttributeComparator>(ref, sortDescription.GetSortPattern() == config::sp_desc);
        _sortComps.emplace_back(std::move(attributeComparator));
    }

    return true;
}

int PackValueComparator::Compare(const autil::StringView& lhs, const autil::StringView& rhs) const
{
    auto leftValue = DecodeValue(lhs);
    auto rightValue = DecodeValue(rhs);
    for (size_t i = 0; i < _sortComps.size(); ++i) {
        const auto& comp = _sortComps[i];
        if (comp->LessThan(leftValue, rightValue)) {
            return -1;
        } else if (comp->LessThan(rightValue, leftValue)) {
            return 1;
            ;
        }
    }
    return 0;
}

autil::StringView PackValueComparator::DecodeValue(const autil::StringView& value) const
{
    if (_fixedValueLen > 0 || !_needDecode) {
        return value;
    } else {
        autil::MultiChar multiChar;
        multiChar.init(value.data());
        return autil::StringView(multiChar.data(), multiChar.size());
    }
}

} // namespace indexlibv2::index
