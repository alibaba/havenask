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
#include "build_service/workflow/DocProcessorProducer.h"

#include <iosfwd>

#include "build_service/document/ClassifiedDocument.h"

using namespace std;
using namespace build_service::document;
namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, DocProcessorProducer);

DocProcessorProducer::DocProcessorProducer(processor::Processor* processor) : _processor(processor) {}

DocProcessorProducer::~DocProcessorProducer() {}

// processor get (produce) processed doc
FlowError DocProcessorProducer::produce(document::ProcessedDocumentVecPtr& processedDocVec)
{
    processedDocVec = _processor->getProcessedDoc();
    if (processedDocVec) {
        return FE_OK;
    }
    return _processor->isSealed() ? FE_SEALED : FE_EOF;
}

bool DocProcessorProducer::seek(const common::Locator& locator) { return true; }

bool DocProcessorProducer::stop(StopOption stopOption)
{
    if (stopOption == SO_INSTANT) {
        _processor->drop();
    }
    return true;
}

void DocProcessorProducer::notifyStop(StopOption stopOption)
{
    if (stopOption == SO_INSTANT) {
        _processor->drop();
    }
}

}} // namespace build_service::workflow
