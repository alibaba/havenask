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
#include "indexlib/base/Types.h"
#include "indexlib/document/IIndexFieldsParser.h"
#include "indexlib/document/aggregation/DeleteValueEncoder.h"
#include "indexlib/document/kv/ValueConvertor.h"
#include "indexlib/index/aggregation/AggregationIndexFields.h"

namespace indexlib::util {
class AccumulativeCounter;
}

namespace indexlibv2::document {
class RawDocument;
class DocumentInitParam;
class DeleteValueEncoder;
} // namespace indexlibv2::document

namespace indexlibv2::config {
class AggregationIndexConfig;
}

namespace indexlibv2::index {
class AttributeConvertor;

class AggregationIndexFieldsParser : public document::IIndexFieldsParser
{
private:
    struct FieldResource {
        uint64_t hash = 0;
        std::shared_ptr<config::AggregationIndexConfig> aggregationConfig;
        std::unique_ptr<document::ValueConvertor> valueConvertor;
        std::unique_ptr<document::DeleteValueEncoder> deleteValueEncoder;
    };

public:
    ~AggregationIndexFieldsParser();

    Status Init(const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
                const std::shared_ptr<document::DocumentInitParam>& initParam) override;
    indexlib::util::PooledUniquePtr<document::IIndexFields> Parse(const document::ExtendDocument& extendDoc,
                                                                  autil::mem_pool::Pool* pool) override;
    indexlib::util::PooledUniquePtr<document::IIndexFields> Parse(autil::StringView serializedData,
                                                                  autil::mem_pool::Pool* pool) override;

private:
    bool ParseSingleField(const FieldResource& fieldResource, const std::shared_ptr<document::RawDocument>& rawDoc,
                          autil::mem_pool::Pool* pool, AggregationIndexFields::SingleField* singleField) const;
    bool ParseKey(const FieldResource& fieldResource, const std::shared_ptr<document::RawDocument>& rawDoc,
                  autil::mem_pool::Pool* pool, AggregationIndexFields::SingleField* singleField) const;
    bool NeedParseValue(const std::shared_ptr<config::AggregationIndexConfig>& aggregationConfig,
                        DocOperateType opType) const;
    document::ValueConvertor::ConvertResult ParseValue(const FieldResource& fieldResource,
                                                       const document::RawDocument& rawDoc,
                                                       autil::mem_pool::Pool* pool) const;
    uint64_t Hash(const std::shared_ptr<config::AggregationIndexConfig>& aggregationConfig,
                  const document::RawDocument& doc) const;
    autil::StringView GetFieldValue(const document::RawDocument& doc, const std::string& fieldName) const;

private:
    std::vector<FieldResource> _fieldResources;
    std::unique_ptr<index::AttributeConvertor> _versionConvertor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
