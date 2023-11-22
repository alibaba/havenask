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
#include "build_service/proto/EffectiveIndexInfo.h"

#include "indexlib/index/ann/Common.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/primary_key/Common.h"

using namespace std;

namespace build_service { namespace proto {
BS_LOG_SETUP(proto, EffectiveIndexInfo);

EffectiveIndexInfo::EffectiveIndexInfo() {}

EffectiveIndexInfo::~EffectiveIndexInfo() {}

void EffectiveIndexInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("indexs", _indexs, _indexs);
    json.Jsonize("attributes", _attributes, _attributes);
}

void EffectiveIndexInfo::addIndex(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    std::map<std::string, std::string> param;
    param["name"] = indexConfig->GetIndexName();

    const auto& indexType = indexConfig->GetIndexType();
    if (indexType == indexlib::index::INVERTED_INDEX_TYPE_STR ||
        indexType == indexlib::index::PRIMARY_KEY_INDEX_TYPE_STR ||
        indexType == indexlibv2::index::ANN_INDEX_TYPE_STR) {
        const auto& invertedConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
        assert(invertedConfig);
        auto invertedIndexType = invertedConfig->GetInvertedIndexType();
        param["type"] = std::string(indexlibv2::config::InvertedIndexConfig::InvertedIndexTypeToStr(invertedIndexType));
        _indexs.push_back(param);
        return;
    }
    if (indexType == indexlib::index::ATTRIBUTE_INDEX_TYPE_STR) {
        const auto& attrConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
        assert(attrConfig);
        auto fieldType = attrConfig->GetFieldType();
        param["type"] = std::string(indexlibv2::config::FieldConfig::FieldTypeToStr(fieldType));
        _attributes.push_back(param);
        return;
    }
}

}} // namespace build_service::proto
