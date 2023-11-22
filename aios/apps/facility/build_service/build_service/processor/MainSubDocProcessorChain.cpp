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
#include "build_service/processor/MainSubDocProcessorChain.h"

#include <assert.h>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "build_service/document/ClassifiedDocument.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"

using namespace std;
using namespace build_service::document;
using namespace indexlib::document;

namespace build_service { namespace processor {

BS_LOG_SETUP(processor, MainSubDocProcessorChain);

MainSubDocProcessorChain::MainSubDocProcessorChain(DocumentProcessorChain* mainDocProcessor,
                                                   DocumentProcessorChain* subDocProcessor)
    : DocumentProcessorChain(mainDocProcessor->getIndexPartitionSchema())
    , _mainDocProcessor(mainDocProcessor)
    , _subDocProcessor(subDocProcessor)
{
}

MainSubDocProcessorChain::MainSubDocProcessorChain(MainSubDocProcessorChain& other) : DocumentProcessorChain(other)
{
    _mainDocProcessor = other._mainDocProcessor->clone();
    _subDocProcessor = other._subDocProcessor->clone();
}

MainSubDocProcessorChain::~MainSubDocProcessorChain()
{
    delete _mainDocProcessor;
    delete _subDocProcessor;
}

bool MainSubDocProcessorChain::processExtendDocument(const ExtendDocumentPtr& extendDocument)
{
    if (!_mainDocProcessor->processExtendDocument(extendDocument)) {
        return false;
    }
    const IndexlibExtendDocumentPtr& indexExtDoc = extendDocument->getLegacyExtendDoc();
    assert(indexExtDoc);
    for (size_t i = 0; i < extendDocument->getSubDocumentsCount();) {
        if (!_subDocProcessor->processExtendDocument(extendDocument->getSubDocument(i))) {
            extendDocument->removeSubDocument(i);
            continue;
        }

        indexExtDoc->addSubDocument(extendDocument->getSubDocument(i)->getLegacyExtendDoc());
        ++i;
    }
    if (indexExtDoc->getSubDocumentsCount() != extendDocument->getSubDocumentsCount()) {
        BS_LOG(ERROR, "subDocCount not match [%lu,%lu]", indexExtDoc->getSubDocumentsCount(),
               extendDocument->getSubDocumentsCount());
        return false;
    }
    return true;
}

void MainSubDocProcessorChain::batchProcessExtendDocs(const vector<ExtendDocumentPtr>& extDocVec)
{
    _mainDocProcessor->batchProcessExtendDocs(extDocVec);
    for (auto& extendDocument : extDocVec) {
        if (extendDocument->testWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH)) {
            continue;
        }

        _subDocProcessor->batchProcessExtendDocs(extendDocument->getAllSubDocuments());
        const IndexlibExtendDocumentPtr& indexExtDoc = extendDocument->getLegacyExtendDoc();
        assert(indexExtDoc);
        for (size_t i = 0; i < extendDocument->getSubDocumentsCount();) {
            if (extendDocument->getSubDocument(i)->testWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH)) {
                extendDocument->removeSubDocument(i);
                continue;
            }
            indexExtDoc->addSubDocument(extendDocument->getSubDocument(i)->getLegacyExtendDoc());
            ++i;
        }
        if (indexExtDoc->getSubDocumentsCount() != extendDocument->getSubDocumentsCount()) {
            BS_LOG(ERROR, "subDocCount not match [%lu,%lu]", indexExtDoc->getSubDocumentsCount(),
                   extendDocument->getSubDocumentsCount());
            extendDocument->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

DocumentProcessorChain* MainSubDocProcessorChain::clone() { return new MainSubDocProcessorChain(*this); }

}} // namespace build_service::processor
