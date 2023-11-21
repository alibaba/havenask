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

#include "build_service/common/ResourceContainer.h"
#include "build_service/local_job/ReduceDocumentQueue.h"
#include "build_service/workflow/BuildFlowMode.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/workflow/FlowFactory.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace local_job {

class LocalBrokerFactory : public workflow::FlowFactory
{
public:
    LocalBrokerFactory(ReduceDocumentQueue* queue);
    ~LocalBrokerFactory();

private:
    LocalBrokerFactory(const LocalBrokerFactory&);
    LocalBrokerFactory& operator=(const LocalBrokerFactory&);

public:
    workflow::ProcessedDocProducer* createProcessedDocProducer(const RoleInitParam& initParamconst,
                                                               const ProducerType& type) override;
    workflow::ProcessedDocConsumer* createProcessedDocConsumer(const RoleInitParam& initParam,
                                                               const ConsumerType& type) override;

    bool initCounterMap(RoleInitParam& initParam, workflow::BuildFlowMode mode) override
    {
        initParam.counterMap.reset(new indexlib::util::CounterMap());
        return true;
    }

private:
    ReduceDocumentQueue* _queue;
    workflow::FlowFactory* _flowFactory;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::local_job
