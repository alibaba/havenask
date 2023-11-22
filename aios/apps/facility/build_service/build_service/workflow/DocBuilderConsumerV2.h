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

#include <stdint.h>

#include "alog/Logger.h"
#include "build_service/builder/BuilderV2.h"
#include "build_service/common/Locator.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/workflow/StopOption.h"

namespace build_service { namespace workflow {

class DocBuilderConsumerV2 : public ProcessedDocConsumer
{
public:
    DocBuilderConsumerV2(builder::BuilderV2* builder);
    ~DocBuilderConsumerV2();

private:
    DocBuilderConsumerV2(const DocBuilderConsumerV2&);
    DocBuilderConsumerV2& operator=(const DocBuilderConsumerV2&);

public:
    FlowError consume(const document::ProcessedDocumentVecPtr& item) override;
    bool getLocator(common::Locator& locator) const override;
    bool stop(FlowError lastError, StopOption stopOption) override;
    void notifyStop(StopOption stopOption) override;

public:
    void SetEndBuildTimestamp(int64_t endBuildTs) override
    {
        BS_LOG(INFO, "DocBuilderConsumerV2 will stop at timestamp[%ld]", endBuildTs);
        _endTimestamp = endBuildTs;
    }

private:
    builder::BuilderV2* _builder;
    volatile int64_t _endTimestamp;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocBuilderConsumerV2);

}} // namespace build_service::workflow
