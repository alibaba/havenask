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
#include "build_service/processor/DocumentProcessorChain.h"

#include "autil/EnvUtil.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/Monitor.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/IDocumentFactory.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/document/query/query_parser_creator.h"
#include "indexlib/util/DocTracer.h"

using namespace std;
using namespace build_service::document;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::common;

using namespace indexlib::document;
using namespace indexlib::util;
using namespace indexlib::config;

namespace build_service { namespace processor {

BS_LOG_SETUP(processor, DocumentProcessorChain);

DocumentProcessorChain::DocumentProcessorChain(const indexlib::config::IndexPartitionSchemaPtr& schema)
    : _schema(schema)
    , _docSerializeVersion(indexlib::INVALID_DOC_VERSION)
    , _tolerateFormatError(true)
    , _useRawDocAsDoc(false)
    , _serializeRawDoc(false)
{
    _traceField = autil::EnvUtil::getEnv(BS_ENV_DOC_TRACE_FIELD, _traceField);
}

DocumentProcessorChain::DocumentProcessorChain(DocumentProcessorChain& other)
    : _docFactoryWrapper(other._docFactoryWrapper)
    , _docParser(other._docParser)
    , _docParserV2(other._docParserV2)
    , _documentFactoryV2(other._documentFactoryV2)
    , _parserFactoryGroup(other._parserFactoryGroup)
    , _schema(other._schema)
    , _srcSchemaParserGroup(other._srcSchemaParserGroup)
    , _docSerializeVersion(other._docSerializeVersion)
    , _tolerateFormatError(other._tolerateFormatError)
    , _useRawDocAsDoc(other._useRawDocAsDoc)
    , _serializeRawDoc(other._serializeRawDoc)
{
}

DocumentProcessorChain::~DocumentProcessorChain() {}

bool DocumentProcessorChain::createSourceDocument(document::ExtendDocumentPtr& extDoc)
{
    if (_srcSchemaParserGroup.empty()) {
        return true;
    }
    const ClassifiedDocumentPtr& classifiedDoc = extDoc->getClassifiedDocument();
    const RawDocumentPtr& rawDoc = extDoc->getRawDocument();
    // TODO, optimize fieldGroups to shared_ptr
    SourceSchema::FieldGroups fieldGroups;
    fieldGroups.resize(_srcSchemaParserGroup.size());
    for (size_t i = 0; i < _srcSchemaParserGroup.size(); i++) {
        if (!_srcSchemaParserGroup[i]->CreateDocSrcSchema(rawDoc, fieldGroups[i])) {
            BS_LOG(ERROR, "create doc src schema failed, rawDoc[%s]", rawDoc->toString().c_str());
            return false;
        }
    }
    classifiedDoc->createSourceDocument(fieldGroups, rawDoc);
    return true;
}

ProcessedDocumentPtr DocumentProcessorChain::process(const RawDocumentPtr& rawDoc)
{
    IE_RAW_DOC_TRACE(rawDoc, "process begin");
    if (!rawDoc->IsUserDoc()) {
        return ProcessedDocumentPtr();
    }
    ExtendDocumentPtr extendDocument = createExtendDocument();
    extendDocument->setRawDocument(rawDoc);
    if (!createSourceDocument(extendDocument)) {
        return ProcessedDocumentPtr();
    }
    if (!processExtendDocument(extendDocument)) {
        return ProcessedDocumentPtr();
    }
    ProcessedDocumentPtr ret = handleExtendDocument(extendDocument);
    if (!_traceField.empty()) {
        string traceValue = rawDoc->getField(_traceField);
        ret->setTraceField(traceValue);
    }
    IE_RAW_DOC_TRACE(rawDoc, "process done");
    return ret;
}

std::unique_ptr<ExtendDocument> DocumentProcessorChain::createExtendDocument() const
{
    if (_documentFactoryV2) {
        std::shared_ptr<indexlibv2::document::ExtendDocument> extendDoc = _documentFactoryV2->CreateExtendDocument();
        return std::make_unique<ExtendDocument>(extendDoc);
    }
    return std::make_unique<ExtendDocument>();
}

ProcessedDocumentVecPtr DocumentProcessorChain::batchProcess(const RawDocumentVecPtr& batchRawDocs)
{
    ExtendDocument::ExtendDocumentVec extDocVec;
    extDocVec.reserve(batchRawDocs->size());

    for (size_t i = 0; i < batchRawDocs->size(); i++) {
        ExtendDocumentPtr extendDocument = createExtendDocument();
        extendDocument->setRawDocument((*batchRawDocs)[i]);
        if (!createSourceDocument(extendDocument) || !(*batchRawDocs)[i]->IsUserDoc()) {
            extendDocument->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
        extDocVec.push_back(extendDocument);
        IE_RAW_DOC_TRACE((*batchRawDocs)[i], "process begin");
    }
    batchProcessExtendDocs(extDocVec);
    ProcessedDocumentVecPtr ret(new ProcessedDocumentVec);
    ret->reserve(batchRawDocs->size());
    for (size_t i = 0; i < extDocVec.size(); i++) {
        ProcessedDocumentPtr processedDoc = handleExtendDocument(extDocVec[i]);
        const RawDocumentPtr& rawDoc = extDocVec[i]->getRawDocument();
        IE_RAW_DOC_TRACE(rawDoc, "process done");
        if (!_traceField.empty()) {
            string traceValue = rawDoc->getField(_traceField);
            processedDoc->setTraceField(traceValue);
        }
        ret->push_back(processedDoc);
    }
    return ret;
}

ProcessedDocumentPtr DocumentProcessorChain::handleExtendDocument(const ExtendDocumentPtr& extendDocument)
{
    if (extendDocument->testWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH)) {
        return ProcessedDocumentPtr();
    }

    const ProcessedDocumentPtr& processedDocument = extendDocument->getProcessedDocument();
    DocOperateType opType = extendDocument->getRawDocument()->getDocOperateType();
    const RawDocumentPtr& rawDoc = extendDocument->getRawDocument();
    processedDocument->setRawDocString(extendDocument->getRawDocString());
    if (_serializeRawDoc) {
        processedDocument->enableSerializeRawDocument();
    }

    if (_queryEvaluator && _queryEvaluator->Evaluate(rawDoc, _evaluateParam) != ES_TRUE) {
        // skip for query evaluate fail, raw doc not match query condition
        processedDocument->setNeedSkip(true);
        IE_RAW_DOC_TRACE(rawDoc, "skip rawDoc for query evaluate fail.");
        return processedDocument;
    }

    if (opType == DocOperateType::SKIP_DOC) {
        // skip doc
        processedDocument->setNeedSkip(true);
        IE_RAW_DOC_TRACE(rawDoc, "skip rawDoc.");
        return processedDocument;
    }
    if (opType == DocOperateType::CHECKPOINT_DOC) {
        // skip checkpoint doc
        processedDocument->setNeedSkip(true);
        IE_RAW_DOC_TRACE(rawDoc, "skip checkpoint rawDoc.");
        return processedDocument;
    }

    std::shared_ptr<indexlibv2::document::IDocumentBatch> docBatch;
    if (_useRawDocAsDoc) {
        auto indexDocument = std::dynamic_pointer_cast<Document>(rawDoc);
        IE_RAW_DOC_TRACE(rawDoc, "use rawDoc as indexDoc.");
        if (!indexDocument) {
            BS_INTERVAL_LOG(300, ERROR, "raw doc cast to document failed!");
        } else {
            if (_docSerializeVersion != indexlib::INVALID_DOC_VERSION) {
                indexDocument->SetSerializedVersion(_docSerializeVersion);
            }
        }
        docBatch = std::make_shared<indexlibv2::document::DocumentBatch>();
        docBatch->AddDocument(indexDocument);
    } else if (_docParserV2) {
        auto [status, batch] = _docParserV2->Parse(*extendDocument->getExtendDoc());
        if (!status.IsOK()) {
            BS_INTERVAL_LOG(300, ERROR, "document parser v2 parse failed!");
        } else if (batch) {
            auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(batch.get());
            while (iter->HasNext()) {
                auto doc = iter->Next();
                doc->SetTrace(extendDocument->getRawDocument()->NeedTrace());
                IE_DOC_TRACE(doc, "parse indexDoc done");
            }
            docBatch.reset(batch.release());
        }
    } else if (_docParser) {
        indexlib::document::DocumentPtr indexDocument = _docParser->Parse(extendDocument->getLegacyExtendDoc());
        if (indexDocument) {
            indexDocument->SetTrace(extendDocument->getRawDocument()->NeedTrace());
            if (_docSerializeVersion != indexlib::INVALID_DOC_VERSION) {
                indexDocument->SetSerializedVersion(_docSerializeVersion);
            }
            docBatch = std::make_shared<indexlibv2::document::DocumentBatch>();
            docBatch->AddDocument(indexDocument);
        }
        IE_INDEX_DOC_TRACE(indexDocument, "parse indexDoc done");
    }

    if (!docBatch) {
        BS_INTERVAL_LOG(300, ERROR, "create document batch fail!");
        IE_RAW_DOC_TRACE(rawDoc, "parse rawDoc failed.");
        return ProcessedDocumentPtr();
    }
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch.get());
    while (iter->HasNext()) {
        auto doc = iter->Next();
        handleBuildInProcessorWarnings(extendDocument, doc);
        doc->SetDocInfo(rawDoc->GetDocInfo());
    }
    processedDocument->setDocumentBatch(docBatch);
    processedDocument->setLocator(extendDocument->getRawDocument()->getLocator());
    return processedDocument;
}

void DocumentProcessorChain::handleBuildInProcessorWarnings(
    const ExtendDocumentPtr& extendDocument, const std::shared_ptr<indexlibv2::document::IDocument>& indexDocument)
{
    assert(extendDocument);
    const std::string& docIdentifier = extendDocument->getIdentifier();
    if (extendDocument->testWarningFlag(ProcessorWarningFlag::PWF_HASH_FIELD_EMPTY)) {
        BS_INTERVAL_LOG2(60, WARN, "hashField of Doc[pkField=%s] is missing", docIdentifier.c_str());
    }

    if (indexDocument->HasFormatError()) {
        IE_DOC_TRACE(indexDocument, "doc has format error(check attributeConvertError)");
        if (!_tolerateFormatError) {
            indexDocument->SetDocOperateType(SKIP_DOC);
            BS_LOG(ERROR, "Doc[pkField=%s] has format error(check attributeConvertError), will be skipped",
                   docIdentifier.c_str());
            BEEPER_FORMAT_INTERVAL_WITHOUT_TAGS_REPORT(
                10, WORKER_PROCESS_ERROR_COLLECTOR_NAME,
                "Doc[pkField=%s] has format error(check attributeConvertError), will be skipped",
                docIdentifier.c_str());
        } else {
            BS_INTERVAL_LOG2(60, WARN,
                             "Doc[pkField=%s] has format error(check attributeConvertError),"
                             "will use default value in related fields",
                             docIdentifier.c_str());
        }
    }
}

bool DocumentProcessorChain::init(const DocumentFactoryWrapperPtr& wrapper, const DocumentInitParamPtr& initParam,
                                  const SourceSchemaParserFactoryGroupPtr& parserFactoryGroup, bool useRawDocAsDoc,
                                  bool serializeRawDoc)
{
    assert(useRawDocAsDoc || !serializeRawDoc);
    _useRawDocAsDoc = useRawDocAsDoc;
    _serializeRawDoc = serializeRawDoc;
    _parserFactoryGroup = parserFactoryGroup;
    _srcSchemaParserGroup = parserFactoryGroup->CreateParserGroup();

    if (!wrapper) {
        return false;
    }
    _docFactoryWrapper = wrapper;
    _docParser.reset(_docFactoryWrapper->CreateDocumentParser(initParam));
    return _docParser.get() != nullptr;
}

bool DocumentProcessorChain::initV2(std::unique_ptr<indexlibv2::document::IDocumentFactory> documentFactory,
                                    const DocumentInitParamPtr& initParam,
                                    const SourceSchemaParserFactoryGroupPtr& parserFactoryGroup,
                                    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
{
    _parserFactoryGroup = parserFactoryGroup;
    _srcSchemaParserGroup = parserFactoryGroup->CreateParserGroup();

    if (!documentFactory) {
        BS_LOG(ERROR, "document factory is nullptr");
        return false;
    }
    _documentFactoryV2.reset(documentFactory.release());
    _docParserV2.reset(_documentFactoryV2->CreateDocumentParser(schema, initParam).release());
    return _docParserV2.get() != nullptr;
}

bool DocumentProcessorChain::initQueryEvaluator(const config::ResourceReaderPtr& resourceReader,
                                                const KeyValueMap& kvMap)
{
    string rawDocQuery;
    rawDocQuery = getValueFromKeyValueMap(kvMap, RAW_DOCUMENT_QUERY_STRING, rawDocQuery);
    if (rawDocQuery.empty()) {
        return true;
    }
    string rawDocQueryType = "json";
    rawDocQueryType = getValueFromKeyValueMap(kvMap, RAW_DOCUMENT_QUERY_TYPE, rawDocQueryType);
    QueryParserPtr queryParser = QueryParserCreator::Create(rawDocQueryType);
    if (!queryParser) {
        BS_LOG(ERROR, "create rawDoc query parser fail, type [%s]", rawDocQueryType.c_str());
        return false;
    }
    QueryBasePtr query = queryParser->Parse(rawDocQuery);
    if (!query) {
        BS_LOG(ERROR, "create rawDoc Query fail, query [%s]", rawDocQuery.c_str());
        return false;
    }
    _evaluatorCreator.reset(new QueryEvaluatorCreator);

    string pluginPath = "";
    if (resourceReader) {
        pluginPath = resourceReader->getPluginPath();
    }
    FunctionExecutorResource resource;
    if (resourceReader) {
        resourceReader->getRawDocumentFunctionResource(resource);
    }
    if (!_evaluatorCreator->Init(pluginPath, resource)) {
        BS_LOG(ERROR, "init query evaluator creator fail, pluginPath [%s], resource [%s]!", pluginPath.c_str(),
               ToJsonString(resource).c_str());
        return false;
    }

    _queryEvaluator = _evaluatorCreator->Create(query);
    if (!_queryEvaluator) {
        BS_LOG(ERROR, "create query evaluator fail, query [%s]", rawDocQuery.c_str());
        return false;
    }
    _evaluateParam.pendingForFieldNotExist = false;
    return true;
}

}} // namespace build_service::processor
