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
#include "indexlib/document/kv/KVKeyExtractor.h"
#include "indexlib/document/kv/ValueConvertor.h"
#include "indexlib/index/kv/KVIndexFields.h"

namespace indexlib::util {
class AccumulativeCounter;
}

namespace indexlibv2::document {
class RawDocument;
class DocumentInitParam;
} // namespace indexlibv2::document

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlibv2::index {

class KVIndexFieldsParser : public document::IIndexFieldsParser
{
private:
    struct FieldResource {
        uint64_t hash = 0;
        std::shared_ptr<config::KVIndexConfig> kvConfig;
        std::unique_ptr<document::KVKeyExtractor> keyExtractor;
        std::unique_ptr<document::ValueConvertor> valueConvertor;
    };

public:
    ~KVIndexFieldsParser();

    Status Init(const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
                const std::shared_ptr<document::DocumentInitParam>& initParam) override;
    indexlib::util::PooledUniquePtr<document::IIndexFields> Parse(const document::ExtendDocument& extendDoc,
                                                                  autil::mem_pool::Pool* pool) override;
    indexlib::util::PooledUniquePtr<document::IIndexFields> Parse(autil::StringView serializedData,
                                                                  autil::mem_pool::Pool* pool) override;

private:
    std::shared_ptr<indexlib::util::AccumulativeCounter>
    InitCounter(const std::shared_ptr<document::DocumentInitParam>& initParam) const;

    bool ParseSingleField(const FieldResource& fieldResource, const std::shared_ptr<document::RawDocument>& rawDoc,
                          autil::mem_pool::Pool* pool, KVIndexFields::SingleField* singleField) const;
    bool ParseKey(const FieldResource& fieldResource, const std::shared_ptr<document::RawDocument>& rawDoc,
                  autil::mem_pool::Pool* pool, KVIndexFields::SingleField* singleField) const;
    bool NeedParseValue(DocOperateType opType) const;
    document::ValueConvertor::ConvertResult ParseValue(const FieldResource& fieldResource,
                                                       const document::RawDocument& rawDoc,
                                                       autil::mem_pool::Pool* pool) const;
    void ParseTTL(const std::shared_ptr<config::KVIndexConfig>& kvConfig, const document::RawDocument& rawDoc,
                  KVIndexFields::SingleField* singleField) const;

private:
    inline static const std::string ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME = "bs.processor.attributeConvertError";

private:
    std::vector<FieldResource> _fieldResources;
    std::shared_ptr<indexlib::util::AccumulativeCounter> _attributeConvertErrorCounter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
