#include "build_service/workflow/FlowFactory.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/builder/BuilderCreator.h"
#include "build_service/workflow/DocReaderProducer.h"
#include "build_service/workflow/DocProcessorProducer.h"
#include "build_service/workflow/DocProcessorConsumer.h"
#include "build_service/workflow/DocBuilderConsumer.h"
#include "build_service/processor/ProcessorConfig.h"
#include "build_service/processor/BatchProcessor.h"
#include <autil/StringUtil.h>
#include <indexlib/misc/metric_provider.h>
#include <indexlib/util/counter/counter_map.h>
#include "build_service/config/CLIOptionNames.h"

IE_NAMESPACE_USE(common);

using namespace std;
IE_NAMESPACE_USE(misc);
using namespace build_service::reader;
using namespace build_service::processor;
using namespace build_service::builder;
using namespace build_service::proto;

namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, FlowFactory);

FlowFactory::FlowFactory(const util::SwiftClientCreatorPtr& swiftClientCreator,
                         const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart)
    : _indexPart(indexPart)
    , _readerCreator(swiftClientCreator)
    , _reader(NULL)
    , _processor(NULL)
    , _builder(NULL)

{
}

FlowFactory::~FlowFactory() {
    DELETE_AND_SET_NULL(_reader);
    DELETE_AND_SET_NULL(_processor);
    DELETE_AND_SET_NULL(_builder);
}

void FlowFactory::collectErrorInfos(std::vector<ErrorInfo> &errorInfos) const {
    fillErrorInfos(errorInfos);

    _readerCreator.fillErrorInfos(errorInfos);
}

RawDocProducer *FlowFactory::createRawDocProducer(const RoleInitParam &initParam) {
    reader::RawDocumentReader *reader = getReader(initParam);
    if (!reader) {
        return NULL;
    }
    string sourceSignatureStr = getValueFromKeyValueMap(initParam.kvMap, config::SRC_SIGNATURE);
    uint64_t srcSignature = 0;
    if (!autil::StringUtil::fromString<uint64_t>(sourceSignatureStr, srcSignature)) {
        BS_LOG(INFO, "srcSignature [%s] illegal", sourceSignatureStr.c_str());
        return NULL;
    }
    return new DocReaderProducer(reader, srcSignature);
}

RawDocConsumer *FlowFactory::createRawDocConsumer(const RoleInitParam &initParam) {
    if (initParam.isReadToBuildFlow) {
        return createRawDocBuilderConsumer(initParam);
    }

    Processor *processor = getProcessor(initParam);
    if (!processor) {
        return NULL;
    }
    return new DocProcessorConsumer(processor);
}
    
RawDocConsumer *FlowFactory::createRawDocBuilderConsumer(const RoleInitParam &initParam) {
    Builder *builder = getBuilder(initParam);
    if (!builder) {
        return NULL;
    }
    return new RawDocBuilderConsumer(builder);
}

ProcessedDocProducer *FlowFactory::createProcessedDocProducer(const RoleInitParam &initParam) {
    Processor *processor = getProcessor(initParam);
    if (!processor) {
        return NULL;
    }
    return new DocProcessorProducer(processor);
}

ProcessedDocConsumer *FlowFactory::createProcessedDocConsumer(const RoleInitParam &initParam) {
    Builder *builder = getBuilder(initParam);
    if (!builder) {
        return NULL;
    }
    DocBuilderConsumer* consumer = new DocBuilderConsumer(builder);
    auto it = initParam.kvMap.find(config::PROCESSED_DOC_SWIFT_STOP_TIMESTAMP);
    if (it != initParam.kvMap.end())
    {
        int64_t stopTimestamp = -1;    
        if (!autil::StringUtil::fromString(it->second, stopTimestamp)) {
            BS_LOG(ERROR, "invalid stopTimestamp[%s]", it->second.c_str());
            return NULL;
        }
        else
        {
            consumer->SetEndBuildTimestamp(stopTimestamp);
        }
    }
    return consumer;
}

reader::RawDocumentReader *FlowFactory::getReader(
        const RoleInitParam &initParam)
{
    if (_reader) {
        return _reader;
    }
    _reader = createReader(initParam);
    if (!_reader) {
        string errorMsg = "create reader failed, kvMap: "
                          + keyValueMapToString(initParam.kvMap);
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_READER, errorMsg, BS_STOP);
    }
    return _reader;
}

reader::RawDocumentReader *FlowFactory::createReader(
        const RoleInitParam &initParam)
{
    return _readerCreator.create(initParam.resourceReader,
                                 initParam.kvMap,
                                 initParam.partitionId,
                                 initParam.metricProvider,
                                 initParam.counterMap,
                                 initParam.schema);
}

processor::Processor *FlowFactory::getProcessor(
        const RoleInitParam &initParam)
{
    if (_processor) {
        return _processor;
    }
    _processor = createProcessor(initParam);
    if (!_processor) {
        string errorMsg = "create processor failed, kvMap: "
                          + keyValueMapToString(initParam.kvMap);
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_PROCESSOR, errorMsg, BS_STOP);
    }
    return _processor;
}

processor::Processor *FlowFactory::createProcessor(
        const RoleInitParam &initParam)
{
    ProcessorConfig processorConfig;
    if (!initParam.resourceReader->getDataTableConfigWithJsonPath(
                    initParam.partitionId.buildid().datatable(),
                    "processor_config", processorConfig))
    {
        string errorMsg = "Get processor_config from [" +
                          initParam.partitionId.buildid().datatable() + "] failed.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    processor::Processor *processor = NULL;
    if (processorConfig.processorStrategyStr == ProcessorConfig::NORMAL_PROCESSOR_STRATEGY) {
        processor = new Processor(processorConfig.processorStrategyParameter);
    } else if (processorConfig.processorStrategyStr == ProcessorConfig::BATCH_PROCESSOR_STRATEGY) {
        processor = new BatchProcessor(processorConfig.processorStrategyParameter);
    } else {
        string errorMsg = "unsupported processor_strategy [" +
                          processorConfig.processorStrategyStr +
                          "] in processor_config.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    if (!processor->start(initParam.resourceReader,
                          initParam.partitionId,
                          initParam.metricProvider,
                          initParam.counterMap))
    {
        delete processor;
        return NULL;
    }
    return processor;
}

bool FlowFactory::initCounterMap(RoleInitParam &initParam)
{
    builder::Builder *builder = getBuilder(initParam);
    if (!builder) {
        return false;
    }
    initParam.counterMap = builder->GetCounterMap();
    return initParam.counterMap;
}

builder::Builder *FlowFactory::getBuilder(
        const RoleInitParam &initParam)
{
    if (_builder) {
        return _builder;
    }
    _builder = createBuilder(initParam);
    if (!_builder) {
        string errorMsg = "create builder failed, kvMap: "
                          + keyValueMapToString(initParam.kvMap);
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_BUILDER, errorMsg, BS_STOP);
    }
    return _builder;

}

builder::Builder *FlowFactory::createBuilder(
        const RoleInitParam &initParam)
{
    BuilderCreator creator(_indexPart);
    return creator.create(initParam.resourceReader, initParam.kvMap,
                          initParam.partitionId, initParam.metricProvider);
}

}
}
