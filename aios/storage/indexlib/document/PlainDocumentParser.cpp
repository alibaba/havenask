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
#include "indexlib/document/PlainDocumentParser.h"

#include "autil/DataBuffer.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/IIndexFields.h"
#include "indexlib/document/IIndexFieldsParser.h"
#include "indexlib/document/PlainDocument.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/util/ErrorLogCollector.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, PlainDocumentParser);

Status PlainDocumentParser::Init(const std::shared_ptr<config::TabletSchema>& schema,
                                 const std::shared_ptr<DocumentInitParam>& initParam)
{
    _schemaId = schema->GetSchemaId();
    for (const auto& indexConfig : schema->GetIndexConfigs()) {
        _indexTypes.insert(indexConfig->GetIndexType());
    }
    auto factoryCreator = index::IndexFactoryCreator::GetInstance();
    assert(factoryCreator);
    auto allFactories = factoryCreator->GetAllFactories();
    for (const auto& indexType : _indexTypes) {
        auto indexConfigs = schema->GetIndexConfigs(indexType);
        assert(!indexConfigs.empty());
        auto iter = allFactories.find(indexType);
        if (iter == allFactories.end()) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "index factory [%s] not registered", indexType.c_str());
        }
        auto factory = iter->second;
        assert(factory);
        auto parser = factory->CreateIndexFieldsParser();
        if (!parser) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "create index fields parser [%s] failed",
                                   indexType.c_str());
        }
        RETURN_IF_STATUS_ERROR(parser->Init(indexConfigs, initParam), "index document parser [%s] init failed",
                               indexType.c_str());
        _indexFieldsParsers[indexType] = std::move(parser);
    }
    return Status::OK();
}

std::unique_ptr<IDocumentBatch> PlainDocumentParser::Parse(const std::string& docString,
                                                           const std::string& docFormat) const
{
    assert(false);
    return {};
}

std::pair<Status, std::unique_ptr<IDocumentBatch>> PlainDocumentParser::Parse(ExtendDocument& extendDoc) const
{
    const auto& rawDoc = extendDoc.getRawDocument();
    auto opType = rawDoc->getDocOperateType();
    if (opType == SKIP_DOC || opType == UNKNOWN_OP) {
        return {Status::OK(), nullptr};
    }

    auto docBatch = std::make_unique<TemplateDocumentBatch<PlainDocument>>();
    auto plainDoc = std::make_shared<PlainDocument>(/*pool*/ nullptr);
    auto pool = plainDoc->GetPool();

    int64_t timestamp = rawDoc->getDocTimestamp();
    auto docInfo = plainDoc->GetDocInfo();
    docInfo.timestamp = timestamp;
    plainDoc->SetDocInfo(docInfo);
    plainDoc->SetSource(rawDoc->getDocSource());
    plainDoc->SetDocOperateType(opType);
    plainDoc->SetIngestionTimestamp(rawDoc->GetIngestionTimestamp());
    plainDoc->SetSchemaId(_schemaId);

    for (const auto& [indexType, indexFieldsParser] : _indexFieldsParsers) {
        auto indexFields = indexFieldsParser->Parse(extendDoc, pool);
        if (!indexFields) {
            AUTIL_LOG(ERROR, "index fields [%s] parse failed", indexType.to_string().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "index fields [%s] parse failed", indexType.to_string().c_str());
            return {Status::InternalError(), nullptr};
        }
        [[maybe_unused]] auto ret = plainDoc->AddIndexFields(std::move(indexFields));
        assert(ret);
    }
    docBatch->AddDocument(std::move(plainDoc));
    return {Status::OK(), std::move(docBatch)};
}

std::pair<Status, std::unique_ptr<IDocumentBatch>>
PlainDocumentParser::Parse(const autil::StringView& serializedData) const
{
    autil::DataBuffer dataBuffer(const_cast<char*>(serializedData.data()), serializedData.length());
    auto docBatch = std::make_unique<TemplateDocumentBatch<PlainDocument>>();
    auto plainDoc = std::make_shared<PlainDocument>(/*pool*/ nullptr);
    try {
        bool hasObj = false;
        dataBuffer.read(hasObj);
        if (!hasObj) {
            RETURN2_IF_STATUS_ERROR(Status::Corruption(), nullptr, "document deserialize failed");
        }
        // TODO: support legacy document format, SERIALIZE_VERSION < 100000
        plainDoc->deserialize(dataBuffer, _indexFieldsParsers);
        docBatch->AddDocument(std::move(plainDoc));
    } catch (const std::exception& e) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), nullptr, "plain document deserialize failed, exception[%s]",
                                e.what());
    } catch (...) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), nullptr, "plain document deserialize failed, unknown exception");
    }
    return {Status::OK(), std::move(docBatch)};
}

std::pair<Status, std::unique_ptr<IDocumentBatch>>
PlainDocumentParser::ParseRawDoc(const std::shared_ptr<RawDocument>& rawDoc) const
{
    assert(false);
    return {};
}

} // namespace indexlibv2::document
