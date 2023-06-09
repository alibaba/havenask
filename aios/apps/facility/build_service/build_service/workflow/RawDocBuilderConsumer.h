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
#ifndef ISEARCH_BS_RAWDOCBUILDERCONSUMER_H
#define ISEARCH_BS_RAWDOCBUILDERCONSUMER_H

#include "build_service/builder/Builder.h"
#include "build_service/common_define.h"
#include "build_service/processor/Processor.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Consumer.h"
namespace build_service { namespace workflow {

class RawDocBuilderConsumer : public RawDocConsumer
{
public:
    RawDocBuilderConsumer(builder::Builder* builder, processor::Processor* processor = nullptr);
    ~RawDocBuilderConsumer();

private:
    RawDocBuilderConsumer(const RawDocBuilderConsumer&);
    RawDocBuilderConsumer& operator=(const RawDocBuilderConsumer&);

public:
    FlowError consume(const document::RawDocumentPtr& item) override;
    bool getLocator(common::Locator& locator) const override;
    bool stop(FlowError lastError, StopOption stopOption) override;

public:
    void SetEndBuildTimestamp(int64_t endBuildTs) override
    {
        BS_LOG(INFO, "RawDocBuilderConsumer will stop at timestamp[%ld]", endBuildTs);
        _endTimestamp = endBuildTs;
    }

private:
    FlowError processAndBuildDoc(const document::RawDocumentPtr& item);
    FlowError buildDoc(const document::RawDocumentPtr& item);

private:
    builder::Builder* _builder;
    processor::Processor* _processor;
    volatile int64_t _endTimestamp;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RawDocBuilderConsumer);

}} // namespace build_service::workflow

#endif // ISEARCH_BS_RAWDOCBUILDERCONSUMER_H
