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
#include "build_service/local_job/LocalBrokerFactory.h"

#include <cstddef>
#include <stdint.h>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/HashAlgorithm.h"
#include "build_service/common_define.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/local_job/LocalProcessedDocConsumer.h"
#include "build_service/local_job/LocalProcessedDocProducer.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/task_base/JobConfig.h"

using namespace std;
using namespace build_service::config;
using namespace build_service::workflow;

namespace build_service { namespace local_job {

BS_LOG_SETUP(local_job, LocalBrokerFactory);

LocalBrokerFactory::LocalBrokerFactory(ReduceDocumentQueue* queue) : _queue(queue) {}

LocalBrokerFactory::~LocalBrokerFactory() {}

workflow::ProcessedDocProducer* LocalBrokerFactory::createProcessedDocProducer(const RoleInitParam& initParam,
                                                                               const ProducerType& type)
{
    switch (type) {
    case TP_PROCESSOR:
        return FlowFactory::createProcessedDocProducer(initParam, type);
    default: {
        string srcSignature = getValueFromKeyValueMap(initParam.kvMap, SRC_SIGNATURE);
        uint64_t src = autil::HashAlgorithm::hashString64(srcSignature.c_str(), srcSignature.size());
        task_base::JobConfig jobConfig;
        if (!jobConfig.init(initParam.kvMap)) {
            string errorMsg = "create LocalProcessedDocConsumer failed : Invalid job config!";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return NULL;
        }
        vector<int32_t> reduceIdTable = jobConfig.calcMapToReduceTable();
        return new LocalProcessedDocProducer(_queue, src, reduceIdTable[initParam.partitionId.range().from()]);
    }
    }
}

workflow::ProcessedDocConsumer* LocalBrokerFactory::createProcessedDocConsumer(const RoleInitParam& initParam,
                                                                               const ConsumerType& type)
{
    switch (type) {
    case TP_BUILDER:
    case TP_BUILDERV2:
        return FlowFactory::createProcessedDocConsumer(initParam, type);
    default: {
        task_base::JobConfig jobConfig;
        if (!jobConfig.init(initParam.kvMap)) {
            string errorMsg = "create LocalProcessedDocConsumer failed : Invalid job config!";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return NULL;
        }
        vector<int32_t> reduceIdTable = jobConfig.calcMapToReduceTable();
        return new LocalProcessedDocConsumer(_queue, reduceIdTable);
    }
    }
}
}} // namespace build_service::local_job
