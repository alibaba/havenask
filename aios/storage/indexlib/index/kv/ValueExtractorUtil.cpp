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
#include "indexlib/index/kv/ValueExtractorUtil.h"

#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/attribute/CompactPackAttributeDecoder.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {

Status ValueExtractorUtil::InitValueExtractor(const indexlibv2::config::KVIndexConfig& kvIndexConfig,
                                              std::shared_ptr<AttributeConvertor>& convertor,
                                              std::shared_ptr<PlainFormatEncoder>& encoder)
{
    const auto& valueConfig = kvIndexConfig.GetValueConfig();
    if (valueConfig->IsSimpleValue()) {
        const auto& attrConfig = valueConfig->GetAttributeConfig(0);
        if (!attrConfig) {
            return Status::ConfigError("no fields in value for index: %s", kvIndexConfig.GetIndexName().c_str());
        }
        convertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
        encoder.reset();
    } else {
        auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
        if (!status.IsOK() || !packAttrConfig) {
            return Status::ConfigError("create pack attribute config failed for index: %s",
                                       kvIndexConfig.GetIndexName().c_str());
        }
        auto packAttrConvertor = AttributeConvertorFactory::GetInstance()->CreatePackAttrConvertor(packAttrConfig);
        if (valueConfig->GetFixedLength() != -1) {
            convertor = std::make_shared<CompactPackAttributeDecoder>(packAttrConvertor);
        } else {
            convertor.reset(packAttrConvertor);
        }
        encoder.reset(PackAttributeFormatter::CreatePlainFormatEncoder(packAttrConfig));
    }
    return Status::OK();
}

} // namespace indexlibv2::index
