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
#include "build_service/workflow/DocHandlerProducer.h"

#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <vector>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Progress.h"
#include "indexlib/indexlib.h"

using namespace std;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, DocHandlerProducer);

DocHandlerProducer::DocHandlerProducer(ProcessedDocHandler* handler) : _handler(handler) {}

DocHandlerProducer::~DocHandlerProducer() {}

FlowError DocHandlerProducer::produce(document::ProcessedDocumentVecPtr& processedDocVec)
{
    int64_t docTimestamp = INVALID_TIMESTAMP;
    document::ProcessedDocumentPtr doc;
    FlowError fe;
    while (true) {
        fe = _handler->getProcessedDoc(doc, docTimestamp);
        if (fe != FE_OK) {
            return fe;
        }
        if (_seekLocator.IsValid() && docTimestamp != INVALID_TIMESTAMP &&
            docTimestamp < _seekLocator.GetOffset().first) {
            continue;
        }
        processedDocVec.reset(new document::ProcessedDocumentVec());
        processedDocVec->push_back(doc);
        break;
    }
    return fe;
}
bool DocHandlerProducer::seek(const common::Locator& locator)
{
    bool disableHandlerSeek = autil::EnvUtil::getEnv("disable_handler_seek", false);
    if (disableHandlerSeek) {
        BS_LOG(WARN, "handler producer seek is disabled");
        return true;
    }
    _seekLocator = locator;
    return true;
}
bool DocHandlerProducer::stop(StopOption stopOption) { return true; }
}} // namespace build_service::workflow
