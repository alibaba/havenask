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
#ifndef ISEARCH_BS_DATAFLOWFACTORY_H
#define ISEARCH_BS_DATAFLOWFACTORY_H

#include "build_service/common_define.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/BuildFlowMode.h"
#include "build_service/workflow/FlowFactory.h"
#include "build_service/workflow/Workflow.h"

namespace build_service { namespace workflow {

class DataFlowFactory
{
public:
    enum DocType { RAW_DOC = 0, PROCESSED_DOC = 1 };
    static std::vector<std::unique_ptr<Workflow>> createDataFlow(const FlowFactory::RoleInitParam& param,
                                                                 BuildFlowMode mode, FlowFactory* factory);

private:
    static ProducerBase* createProducer(DocType docType, FlowFactory* factory, const FlowFactory::RoleInitParam& param,
                                        FlowFactory::ProducerType);
    static ConsumerBase* createConsumer(DocType docType, FlowFactory* factory, const FlowFactory::RoleInitParam& param,
                                        FlowFactory::ConsumerType);
    static std::pair<std::vector<FlowFactory::ProducerType>, std::vector<FlowFactory::ConsumerType>>
    createFlowType(BuildFlowMode mode, const FlowFactory::RoleInitParam& param);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DataFlowFactory);

}} // namespace build_service::workflow

#endif // ISEARCH_BS_DATAFLOWFACTORY_H
