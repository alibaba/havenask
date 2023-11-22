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

#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/document/RawDocument.h"
#include "build_service/util/Log.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/document_init_param.h"
#include "indexlib/document/document_parser.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/document/query/query_evaluator.h"
#include "indexlib/document/query/query_evaluator_creator.h"

namespace indexlibv2::document {
class IDocumentParser;
class IDocumentFactory;
} // namespace indexlibv2::document

namespace build_service { namespace processor {

class DocumentProcessorChain
{
public:
    typedef indexlib::document::DocumentRewriter IndexDocumentRewriter;
    typedef std::shared_ptr<IndexDocumentRewriter> IndexDocumentRewriterPtr;
    typedef std::vector<IndexDocumentRewriterPtr> IndexDocumentRewriterVector;

public:
    explicit DocumentProcessorChain(const indexlib::config::IndexPartitionSchemaPtr& schema);
    DocumentProcessorChain(DocumentProcessorChain& other);
    virtual ~DocumentProcessorChain();

public:
    // only return NULL when process failed
    document::ProcessedDocumentPtr process(const document::RawDocumentPtr& rawDoc);

    document::ProcessedDocumentVecPtr batchProcess(const document::RawDocumentVecPtr& batchRawDocs);

    virtual DocumentProcessorChain* clone() = 0;

    void setTolerateFieldFormatError(bool tolerateFormatError) { _tolerateFormatError = tolerateFormatError; }

    void setDocumentSerializeVersion(uint32_t serializeVersion) { _docSerializeVersion = serializeVersion; }

    bool init(const indexlib::document::DocumentFactoryWrapperPtr& wrapper,
              const indexlib::document::DocumentInitParamPtr& initParam, bool useRawDocAsDoc = false,
              bool serializeRawDoc = false);

    bool initV2(std::unique_ptr<indexlibv2::document::IDocumentFactory> documentFactory,
                const indexlib::document::DocumentInitParamPtr& initParam, bool needOriginalSnapshot,
                const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema);

    bool initQueryEvaluator(const config::ResourceReaderPtr& resourceReader, const KeyValueMap& kvMap);
    indexlib::config::IndexPartitionSchemaPtr getIndexPartitionSchema() { return _schema; }

public:
    virtual bool processExtendDocument(const document::ExtendDocumentPtr& extendDocument) = 0;
    virtual void batchProcessExtendDocs(const std::vector<document::ExtendDocumentPtr>& extDocVec) = 0;

protected:
    document::ProcessedDocumentPtr handleExtendDocument(const document::ExtendDocumentPtr& extendDocument);

private:
    void handleBuildInProcessorWarnings(const document::ExtendDocumentPtr& extendDocument,
                                        const std::shared_ptr<indexlibv2::document::IDocument>& indexDocument);
    bool createSourceDocument(document::ExtendDocumentPtr& extDoc);
    std::unique_ptr<document::ExtendDocument> createExtendDocument() const;

private:
    indexlib::document::QueryEvaluatorCreatorPtr _evaluatorCreator;
    indexlib::document::QueryEvaluatorPtr _queryEvaluator;
    indexlib::document::QueryEvaluator::EvaluateParam _evaluateParam;
    indexlib::document::DocumentFactoryWrapperPtr _docFactoryWrapper;
    indexlib::document::DocumentParserPtr _docParser;
    std::shared_ptr<indexlibv2::document::IDocumentParser> _docParserV2;
    std::shared_ptr<indexlibv2::document::IDocumentFactory> _documentFactoryV2;
    indexlib::config::IndexPartitionSchemaPtr _schema;

    uint32_t _docSerializeVersion;
    bool _tolerateFormatError;
    bool _useRawDocAsDoc = false;
    bool _serializeRawDoc = false;
    bool _needOriginalSnapshot = false;
    std::string _traceField;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocumentProcessorChain);
typedef std::vector<DocumentProcessorChainPtr> DocumentProcessorChainVec;
BS_TYPEDEF_PTR(DocumentProcessorChainVec);

}} // namespace build_service::processor
