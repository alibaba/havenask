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
#include "indexlib/index/common/field_format/field_meta/FieldMetaConvertor.h"

#include "indexlib/base/Status.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, FieldMetaConvertor);

bool FieldMetaConvertor::Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    auto fieldMetaConfig = std::dynamic_pointer_cast<FieldMetaConfig>(indexConfig);
    assert(fieldMetaConfig);
    auto fieldConfigs = fieldMetaConfig->GetFieldConfigs();
    assert(1u == fieldConfigs.size());
    auto fieldConfig = fieldConfigs[0];
    if (fieldConfig->GetFieldType() == ft_text) {
        return true;
    }
    auto attrConfig = std::make_shared<indexlibv2::index::AttributeConfig>();
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "attribute init from field index [%s]  failed", fieldConfig->GetFieldName().c_str());
        return false;
    }
    _attributeConvertor.reset(
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    if (!_attributeConvertor) {
        AUTIL_LOG(ERROR, "unsupport field type [%d], name is [%s]", fieldConfig->GetFieldType(),
                  fieldConfig->GetFieldName().c_str());
        return false;
    }
    return true;
}

autil::StringView FieldMetaConvertor::Encode(const autil::StringView& fieldValue, autil::mem_pool::Pool* memPool,
                                             size_t tokenCount)
{
    bool hasFormatError;
    autil::StringView convertedValue = _attributeConvertor
                                           ? _attributeConvertor->Encode(fieldValue, memPool, hasFormatError)
                                           : autil::StringView::empty_instance();

    char* data = (char*)memPool->allocate(sizeof(size_t) + convertedValue.size());
    *((size_t*)data) = tokenCount;
    convertedValue.copy(data + sizeof(size_t), convertedValue.size());
    return autil::StringView(data, sizeof(size_t) + convertedValue.size());
}

std::pair<bool, FieldValueMeta> FieldMetaConvertor::Decode(const autil::StringView& fieldValue)
{
    FieldValueMeta fieldValueMeta;
    assert(fieldValue.size() >= sizeof(size_t));
    fieldValueMeta.tokenCount = *((size_t*)fieldValue.data());
    if (_attributeConvertor) {
        bool ret =
            _attributeConvertor->DecodeLiteralField(fieldValue.substr(sizeof(size_t)), fieldValueMeta.fieldValue);
        return {ret, fieldValueMeta};
    }

    return {true, fieldValueMeta};
}

} // namespace indexlib::index
