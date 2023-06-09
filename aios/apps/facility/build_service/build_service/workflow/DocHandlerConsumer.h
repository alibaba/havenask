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
#ifndef ISEARCH_BS_DOCHANDLERCONSUMER_H
#define ISEARCH_BS_DOCHANDLERCONSUMER_H

#include "build_service/common_define.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/workflow/ProcessedDocHandler.h"

namespace build_service { namespace workflow {

class DocHandlerConsumer : public ProcessedDocConsumer
{
public:
    DocHandlerConsumer(ProcessedDocHandler* handler);
    ~DocHandlerConsumer();

private:
    DocHandlerConsumer(const DocHandlerConsumer&);
    DocHandlerConsumer& operator=(const DocHandlerConsumer&);

public:
    FlowError consume(const document::ProcessedDocumentVecPtr& item) override;
    bool getLocator(common::Locator& locator) const override;
    bool stop(FlowError lastError, StopOption stopOption) override;

private:
    ProcessedDocHandler* _handler;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocHandlerConsumer);

}} // namespace build_service::workflow

#endif // ISEARCH_BS_DOCHANDLERCONSUMER_H
