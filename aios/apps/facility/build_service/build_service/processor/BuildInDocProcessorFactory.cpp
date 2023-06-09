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
#include "build_service/processor/BuildInDocProcessorFactory.h"

#include "build_service/processor/BinaryDocumentProcessor.h"
#include "build_service/processor/DefaultValueProcessor.h"
#include "build_service/processor/DistributeDocumentProcessor.h"
#include "build_service/processor/DocTTLProcessor.h"
#include "build_service/processor/DocTraceProcessor.h"
#include "build_service/processor/DocumentClusterFilterProcessor.h"
#include "build_service/processor/DupFieldProcessor.h"
#include "build_service/processor/EncodingConvertDocumentProcessor.h"
#include "build_service/processor/FastSlowQueueProcessor.h"
#include "build_service/processor/FlipCharProcessor.h"
#include "build_service/processor/HashDocumentProcessor.h"
#include "build_service/processor/HashFilterProcessor.h"
#include "build_service/processor/LineDataDocumentProcessor.h"
#include "build_service/processor/ModifiedFieldsDocumentProcessor.h"
#include "build_service/processor/MultiValueFieldProcessor.h"
#include "build_service/processor/RegionDocumentProcessor.h"
#include "build_service/processor/SelectBuildTypeProcessor.h"
#include "build_service/processor/SkipProcessor.h"
#include "build_service/processor/SleepProcessor.h"
#include "build_service/processor/SlowUpdateProcessor.h"
#include "build_service/processor/SourceDocumentAccessaryFieldProcessor.h"
#include "build_service/processor/SourceFieldExtractorProcessor.h"
#include "build_service/processor/SubDocumentExtractorProcessor.h"
#include "build_service/processor/TaoliveBusinessProcessor.h"
#include "build_service/processor/TokenCombinedDocumentProcessor.h"
#include "build_service/processor/TokenizeDocumentProcessor.h"

using namespace std;
namespace build_service { namespace processor {

BS_LOG_SETUP(processor, BuildInDocProcessorFactory);

BuildInDocProcessorFactory::BuildInDocProcessorFactory() {}

BuildInDocProcessorFactory::~BuildInDocProcessorFactory() {}

DocumentProcessor* BuildInDocProcessorFactory::createDocumentProcessor(const std::string& processorName)
{
#define ENUM_PROCESSOR(processor)                                                                                      \
    if (processorName == processor::PROCESSOR_NAME) {                                                                  \
        return new processor;                                                                                          \
    }

    ENUM_PROCESSOR(TokenizeDocumentProcessor);
    ENUM_PROCESSOR(BinaryDocumentProcessor);
    ENUM_PROCESSOR(EncodingConvertDocumentProcessor);
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
    ENUM_PROCESSOR(SourceDocumentAccessaryFieldProcessor);
    ENUM_PROCESSOR(FastSlowQueueProcessor);
    ENUM_PROCESSOR(SourceFieldExtractorProcessor);
    ENUM_PROCESSOR(HashFilterProcessor);
    ENUM_PROCESSOR(FlipCharProcessor);
    ENUM_PROCESSOR(SkipProcessor);
    ENUM_PROCESSOR(TaoliveBusinessProcessor);

    string errorMsg = "Unknown build in processor name[" + processorName + "]";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return NULL;
}

}} // namespace build_service::processor
