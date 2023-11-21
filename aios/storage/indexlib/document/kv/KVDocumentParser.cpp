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
#include "indexlib/document/kv/KVDocumentParser.h"

#include "autil/DataBuffer.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/document/BuiltinParserInitParam.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/document/kv/KVIndexDocumentParser.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/util/DocTracer.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, KVDocumentParser);

KVDocumentParser::KVDocumentParser() = default;

KVDocumentParser::~KVDocumentParser() = default;

Status KVDocumentParser::Init(const std::shared_ptr<config::ITabletSchema>& schema,
                              const std::shared_ptr<DocumentInitParam>& initParam)
{
    _schemaId = schema->GetSchemaId();
    _counter = InitCounter(initParam);
    AUTIL_LOG(INFO, "init kv parser schemaid [%d]", _schemaId);

    for (const auto& indexConfig : schema->GetIndexConfigs()) {
        auto indexDocParser = CreateIndexFieldsParser(indexConfig);
        auto s = indexDocParser->Init(indexConfig);
        if (!s.IsOK()) {
            AUTIL_LOG(ERROR, "init index document parser for [%s:%s] failed, error: %s",
                      indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str(), s.ToString().c_str());
            return s;
        }
        auto hash = config::IndexConfigHash::Hash(indexConfig);
        _indexDocParsers.emplace_back(hash, std::move(indexDocParser));
    }

    return Status::OK();
}

std::unique_ptr<IDocumentBatch> KVDocumentParser::Parse(const std::string& docString,
                                                        const std::string& docFormat) const
{
    assert(false);
    return {};
}

std::pair<Status, std::unique_ptr<IDocumentBatch>> KVDocumentParser::BatchParse(IMessageIterator* inputIterator) const
{
    assert(false);
    return {Status::Unimplement(), nullptr};
}

std::pair<Status, std::unique_ptr<IDocumentBatch>> KVDocumentParser::Parse(ExtendDocument& extendDoc) const
{
    const auto& rawDoc = extendDoc.GetRawDocument();
    auto opType = rawDoc->getDocOperateType();
    if (opType == SKIP_DOC || opType == UNKNOWN_OP) {
        return {Status::OK(), nullptr};
    } else if (opType == DELETE_SUB_DOC) {
        ERROR_COLLECTOR_LOG(ERROR, "unsupported document cmd type [%d]", opType);
        IE_RAW_DOC_FORMAT_TRACE(rawDoc, "unsupported document cmd type [%d]", opType);
        return {Status::InternalError("unsupported document cmd type: [%d]", opType), nullptr};
    }

    auto docBatch = CreateDocumentBatch();
    auto pool = docBatch->GetPool();

    bool needIndexHash = _indexDocParsers.size() > 1;
    for (const auto& [hash, indexDocParser] : _indexDocParsers) {
        auto result = indexDocParser->Parse(*rawDoc, pool);
        if (!result.IsOK()) {
            return {result.steal_error(), nullptr};
        }
        auto kvDoc = std::move(result.steal_value());
        if (kvDoc) {
            kvDoc->SetSchemaId(_schemaId);
            if (needIndexHash) {
                kvDoc->SetIndexNameHash(hash);
            }
            docBatch->AddDocument(std::move(kvDoc));
        }
    }
    return {Status::OK(), std::move(docBatch)};
}

std::pair<Status, std::unique_ptr<IDocumentBatch>>
KVDocumentParser::Parse(const autil::StringView& serializedData) const
{
    autil::DataBuffer dataBuffer(const_cast<char*>(serializedData.data()), serializedData.length());
    std::unique_ptr<KVDocumentBatch> docBatch;
    try {
        dataBuffer.read(docBatch);
    } catch (const std::exception& e) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), nullptr, "kv document deserialize failed, exception[%s]",
                                e.what());
    } catch (...) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), nullptr, "kv document deserialize failed, unknown exception");
    }
    // TODO: support legacy kv document serialize format ?
    // if (kvDoc->NeedDeserializeFromLegacy()) {
    return {Status::OK(), std::move(docBatch)};
}

std::pair<Status, std::unique_ptr<IDocumentBatch>>
KVDocumentParser::ParseRawDoc(const std::shared_ptr<RawDocument>& rawDoc) const
{
    assert(false);
    return {};
}

std::unique_ptr<KVIndexDocumentParserBase>
KVDocumentParser::CreateIndexFieldsParser(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    if (indexConfig->GetIndexType() != index::KV_INDEX_TYPE_STR) {
        AUTIL_LOG(ERROR, "unexpected index[%s:%s] in kv table", indexConfig->GetIndexName().c_str(),
                  indexConfig->GetIndexType().c_str());
        return nullptr;
    }
    return std::make_unique<KVIndexDocumentParser>(_counter);
}

std::unique_ptr<KVDocumentBatch> KVDocumentParser::CreateDocumentBatch() const
{
    return std::make_unique<KVDocumentBatch>();
}

std::shared_ptr<indexlib::util::AccumulativeCounter>
KVDocumentParser::InitCounter(const std::shared_ptr<DocumentInitParam>& initParam) const
{
    if (!initParam) {
        return nullptr;
    }

    auto param = std::dynamic_pointer_cast<BuiltInParserInitParam>(initParam);
    if (!param) {
        AUTIL_LOG(WARN, "can not use BuiltInParserInitParam");
        return nullptr;
    }

    const BuiltInParserInitResource& resource = param->GetResource();

    if (resource.counterMap) {
        return resource.counterMap->GetAccCounter(ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME);
    }
    return nullptr;
}

} // namespace indexlibv2::document
