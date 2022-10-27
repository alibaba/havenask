#include "build_service/processor/BuildInDocProcessorFactory.h"
#include "build_service/processor/BinaryDocumentProcessor.h"
#include "build_service/processor/SubDocumentExtractorProcessor.h"
#include "build_service/processor/TokenizeDocumentProcessor.h"
#include "build_service/processor/ModifiedFieldsDocumentProcessor.h"
#include "build_service/processor/TokenCombinedDocumentProcessor.h"
#include "build_service/processor/RegionDocumentProcessor.h"
#include "build_service/processor/HashDocumentProcessor.h"
#include "build_service/processor/DistributeDocumentProcessor.h"
#include "build_service/processor/SelectBuildTypeProcessor.h"
#include "build_service/processor/SleepProcessor.h"
#include "build_service/processor/DocTTLProcessor.h"
#include "build_service/processor/DupFieldProcessor.h"
#include "build_service/processor/DocTraceProcessor.h"
#include "build_service/processor/LineDataDocumentProcessor.h"
#include "build_service/processor/MultiValueFieldProcessor.h"
#include "build_service/processor/DocumentClusterFilterProcessor.h"
#include "build_service/processor/SlowUpdateProcessor.h"
#include "build_service/processor/DefaultValueProcessor.h"

using namespace std;
namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, BuildInDocProcessorFactory);

BuildInDocProcessorFactory::BuildInDocProcessorFactory() {
}

BuildInDocProcessorFactory::~BuildInDocProcessorFactory() {
}

DocumentProcessor* BuildInDocProcessorFactory::createDocumentProcessor(
        const std::string &processorName)
{
#define ENUM_PROCESSOR(processor)                       \
    if (processorName == processor::PROCESSOR_NAME) {   \
        return new processor;                           \
    }

    ENUM_PROCESSOR(TokenizeDocumentProcessor);
    ENUM_PROCESSOR(BinaryDocumentProcessor);
    ENUM_PROCESSOR(SubDocumentExtractorProcessor);
    ENUM_PROCESSOR(ModifiedFieldsDocumentProcessor);
    ENUM_PROCESSOR(TokenCombinedDocumentProcessor);
    ENUM_PROCESSOR(RegionDocumentProcessor);
    ENUM_PROCESSOR(HashDocumentProcessor);
    ENUM_PROCESSOR(DistributeDocumentProcessor);
    ENUM_PROCESSOR(SelectBuildTypeProcessor);
    ENUM_PROCESSOR(SleepProcessor);
    ENUM_PROCESSOR(DocTTLProcessor);
    ENUM_PROCESSOR(DupFieldProcessor);
    ENUM_PROCESSOR(DocTraceProcessor);
    ENUM_PROCESSOR(LineDataDocumentProcessor);
    ENUM_PROCESSOR(MultiValueFieldProcessor);
    ENUM_PROCESSOR(DocumentClusterFilterProcessor);
    ENUM_PROCESSOR(SlowUpdateProcessor);
    ENUM_PROCESSOR(DefaultValueProcessor);


    string errorMsg = "Unknown build in processor name[" + processorName + "]";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return NULL;
}

}
}
