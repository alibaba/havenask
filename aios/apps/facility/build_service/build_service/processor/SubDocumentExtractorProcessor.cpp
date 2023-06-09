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
#include "build_service/processor/SubDocumentExtractorProcessor.h"

#include "build_service/document/RawDocument.h"
#include "indexlib/config/legacy_schema_adapter.h"

using namespace build_service::config;
using namespace build_service::document;
using namespace indexlib::config;

namespace build_service { namespace processor {
namespace {
using std::string;
}
BS_LOG_SETUP(processor, SubDocumentExtractorProcessor);

const string SubDocumentExtractorProcessor::PROCESSOR_NAME = "SubDocumentExtractorProcessor";

SubDocumentExtractorProcessor::SubDocumentExtractorProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
{
}

SubDocumentExtractorProcessor::~SubDocumentExtractorProcessor() {}

bool SubDocumentExtractorProcessor::process(const ExtendDocumentPtr& document)
{
    assert(_subDocumentExtractor);
    assert(document);

    const RawDocumentPtr& origRawDocument = document->getRawDocument();
    assert(origRawDocument);

    std::vector<RawDocumentPtr> subRawDocuments;

    _subDocumentExtractor->extractSubDocuments(origRawDocument, &subRawDocuments);

    DocOperateType opType = document->getRawDocument()->getDocOperateType();

    for (size_t i = 0; i < subRawDocuments.size(); ++i) {
        subRawDocuments[i]->setDocOperateType(opType);
        ExtendDocumentPtr subDoc(new ExtendDocument());
        subDoc->setRawDocument(subRawDocuments[i]);
        document->addSubDocument(subDoc);
    }

    return true;
}

void SubDocumentExtractorProcessor::batchProcess(const std::vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void SubDocumentExtractorProcessor::destroy() { delete this; }

DocumentProcessor* SubDocumentExtractorProcessor::clone() { return new SubDocumentExtractorProcessor(*this); }

bool SubDocumentExtractorProcessor::init(const DocProcessorInitParam& param)
{
    assert(param.schema || param.schemaPtr);
    IndexPartitionSchemaPtr schema;
    if (param.schemaPtr) {
        schema = param.schemaPtr;
    } else {
        auto legacySchemaAdapter = std::dynamic_pointer_cast<LegacySchemaAdapter>(param.schema);
        if (!legacySchemaAdapter) {
            BS_LOG(ERROR, "[%s] is not legacy schema", param.schema->GetTableName().c_str());
            return false;
        }
        schema = legacySchemaAdapter->GetLegacySchema();
    }
    _subDocumentExtractor.reset(new SubDocumentExtractor(schema));
    return true;
}

}} // namespace build_service::processor
