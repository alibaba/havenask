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
#ifndef ISEARCH_BS_DOCUMENT_PROCESSOR_MODULE_FACTORY_H
#define ISEARCH_BS_DOCUMENT_PROCESSOR_MODULE_FACTORY_H

#include <memory>

#include "build_service/plugin/ModuleFactory.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service { namespace processor {

class DocumentProcessorFactory : public plugin::ModuleFactory
{
public:
    DocumentProcessorFactory() {}
    virtual ~DocumentProcessorFactory() {}

public:
    virtual bool init(const KeyValueMap& parameters) = 0;
    virtual void destroy() = 0;

    virtual DocumentProcessor* createDocumentProcessor(const std::string& processorName) = 0;

private:
};

typedef std::shared_ptr<DocumentProcessorFactory> DocumentProcessorFactoryPtr;

}} // namespace build_service::processor

#endif // ISEARCH_BS_DOCUMENT_PROCESSOR_MODULE_FACTORY_H
