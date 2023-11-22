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
#include <vector>

#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/common_define.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/processor/DocumentProcessorChain.h"
#include "build_service/util/Log.h"
#include "indexlib/document/document_parser.h"

namespace build_service { namespace processor {

class DocumentProcessor;

class SingleDocProcessorChain : public DocumentProcessorChain
{
public:
    SingleDocProcessorChain(const plugin::PlugInManagerPtr& pluginManagerPtr,
                            const indexlib::config::IndexPartitionSchemaPtr& schema,
                            const std::shared_ptr<analyzer::AnalyzerFactory>& analyzerFactory);
    SingleDocProcessorChain(SingleDocProcessorChain& other);
    ~SingleDocProcessorChain();

public:
    DocumentProcessorChain* clone() override;

public:
    // for init
    void addDocumentProcessor(DocumentProcessor* processor);

protected:
    bool processExtendDocument(const document::ExtendDocumentPtr& extendDocument) override;

    void batchProcessExtendDocs(const std::vector<document::ExtendDocumentPtr>& extDocVec) override;

public:
    // for test
    const std::vector<DocumentProcessor*>& getDocumentProcessors() { return _documentProcessors; }

private:
    plugin::PlugInManagerPtr _pluginManagerPtr; // hold plugin so for processor.
    std::vector<DocumentProcessor*> _documentProcessors;
    std::shared_ptr<analyzer::AnalyzerFactory> _analyzerFactory; // hold analyzer for processor

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleDocProcessorChain);

}} // namespace build_service::processor
