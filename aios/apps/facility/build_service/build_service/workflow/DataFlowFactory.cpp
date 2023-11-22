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
#include "build_service/workflow/DataFlowFactory.h"

#include <assert.h>
#include <cstddef>

#include "alog/Logger.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ProcessorConfigReader.h"
#include "build_service/config/SrcNodeConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/workflow/SrcDataNode.h"

using namespace std;
using namespace build_service::config;
namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, DataFlowFactory);

ProducerBase* DataFlowFactory::createProducer(DocType docType, FlowFactory* factory,
                                              const FlowFactory::RoleInitParam& param, FlowFactory::ProducerType type)
{
    BS_LOG(INFO, "create producer, docType [%s], producerType [%d]", docType == RAW_DOC ? "raw" : "processed",
           int(type));
    assert(factory);
    switch (docType) {
    case RAW_DOC:
        return factory->createRawDocProducer(param, type);
    case PROCESSED_DOC:
        return factory->createProcessedDocProducer(param, type);
    case BATCH_DOC:
        return factory->createBatchDocProducer(param);
    default:
        assert(false);
    }
    return nullptr;
}

std::string DataFlowFactory::DocTypeToString(DocType docType)
{
    switch (docType) {
    case RAW_DOC:
        return "RAW_DOC";
    case PROCESSED_DOC:
        return "PROCESSED_DOC";
    case BATCH_DOC:
        return "BATCH_DOC";
    default:
        return "UNKNOWN";
    }
    return "UNKNOWN";
}

ConsumerBase* DataFlowFactory::createConsumer(DocType docType, FlowFactory* factory,
                                              const FlowFactory::RoleInitParam& param, FlowFactory::ConsumerType type)
{
    BS_LOG(INFO, "create consumer, docType [%s], consumerType [%d]", DocTypeToString(docType).c_str(), int(type));
    assert(factory);
    switch (docType) {
    case RAW_DOC:
        return factory->createRawDocConsumer(param, type);
    case PROCESSED_DOC:
        return factory->createProcessedDocConsumer(param, type);
    case BATCH_DOC:
        return factory->createBatchDocConsumer(param);
    default:
        assert(false);
    }
    return nullptr;
}

pair<vector<FlowFactory::ProducerType>, vector<FlowFactory::ConsumerType>>
DataFlowFactory::createFlowType(BuildFlowMode mode, const FlowFactory::RoleInitParam& param)
{
    vector<FlowFactory::ConsumerType> consumer;
    vector<FlowFactory::ProducerType> producer;
    auto inputDocType = getValueFromKeyValueMap(param.kvMap, INPUT_DOC_TYPE);
    if (inputDocType == INPUT_DOC_RAW) {
        producer.push_back(FlowFactory::ProducerType::TP_RAW);
    } else if (inputDocType == INPUT_DOC_BATCHDOC) {
        producer.push_back(FlowFactory::ProducerType::TP_DOC_BATCH);
    } else {
        producer.push_back(FlowFactory::ProducerType::TP_MESSAGE_QUEUE);
    }

    if (mode & PROCESSOR) {
        SrcNodeConfig srcConfig;
        ProcessorConfigReaderPtr reader(
            new ProcessorConfigReader(param.resourceReader, param.partitionId.buildid().datatable(),
                                      getValueFromKeyValueMap(param.kvMap, config::PROCESSOR_CONFIG_NODE)));
        if (!SrcDataNode::readSrcConfig(reader, srcConfig)) {
            BS_LOG(ERROR, "failed to read src config, create flow type failed");
            return {};
        }
        if (srcConfig.needSrcNode()) {
            consumer.push_back(FlowFactory::ConsumerType::TP_SRC);
            producer.push_back(FlowFactory::ProducerType::TP_SRC);
        } else {
            consumer.push_back(FlowFactory::ConsumerType::TP_PROCESSOR);
            producer.push_back(FlowFactory::ProducerType::TP_PROCESSOR);
        }
    }
    if (mode & BUILDER) {
        if (param.isTablet) {
            consumer.push_back(FlowFactory::ConsumerType::TP_BUILDERV2);
        } else {
            consumer.push_back(FlowFactory::ConsumerType::TP_BUILDER);
        }
    } else {
        consumer.push_back(FlowFactory::ConsumerType::TP_MESSAGE_QUEUE);
    }
    return make_pair(producer, consumer);
}

vector<unique_ptr<Workflow>> DataFlowFactory::createDataFlow(const FlowFactory::RoleInitParam& param,
                                                             BuildFlowMode mode, FlowFactory* factory)
{
    auto flowType = createFlowType(mode, param);
    vector<FlowFactory::ProducerType>& producerType = flowType.first;
    vector<FlowFactory::ConsumerType>& consumerType = flowType.second;
    assert(producerType.size() == consumerType.size());
    vector<unique_ptr<Workflow>> ret;
    for (size_t i = 0; i < producerType.size(); ++i) {
        DocType docType = PROCESSED_DOC;
        if (i == 0) {
            switch (producerType[i]) {
            case FlowFactory::ProducerType::TP_RAW: {
                docType = RAW_DOC;
                break;
            }
            case FlowFactory::ProducerType::TP_DOC_BATCH: {
                docType = BATCH_DOC;
                break;
            }
            default:
                docType = PROCESSED_DOC;
            };
        }
        unique_ptr<ProducerBase> producer(createProducer(docType, factory, param, producerType[i]));
        unique_ptr<ConsumerBase> consumer(createConsumer(docType, factory, param, consumerType[i]));
        if (!producer.get() || !consumer.get()) {
            BS_LOG(ERROR, "create flow failed");
            return {};
        } else {
            ret.push_back(make_unique<Workflow>(producer.release(), consumer.release()));
        }
    }
    return ret;
}

}} // namespace build_service::workflow
