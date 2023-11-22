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
#include "build_service/processor/DocTTLProcessor.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/document/ClassifiedDocument.h"
#include "build_service/document/RawDocument.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/indexlib.h"

using namespace std;
using namespace autil;
using namespace build_service::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, DocTTLProcessor);
const std::string DocTTLProcessor::PROCESSOR_NAME = "DocTTLProcessor";
DocTTLProcessor::DocTTLProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
    , _ttlTime(0)
    , _ttlFieldName(DOC_TIME_TO_LIVE_IN_SECONDS)
{
}

DocTTLProcessor::~DocTTLProcessor() {}

bool DocTTLProcessor::init(const DocProcessorInitParam& param)
{
    string ttlTimeStr = getValueFromKeyValueMap(param.parameters, "ttl_in_second");
    if (ttlTimeStr.empty() || !StringUtil::fromString(ttlTimeStr, _ttlTime)) {
        BS_LOG(ERROR, "[ttl_in_second] must be specified as a integer");
        return false;
    }

    auto legacySchemaAdapter = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(param.schema);
    if (legacySchemaAdapter) {
        _ttlFieldName = legacySchemaAdapter->GetLegacySchema()->GetTTLFieldName();
    } else {
        if (auto s = param.schema->GetRuntimeSettings().GetValue<std::string>("ttl_field_name"); s.first.IsOK()) {
            _ttlFieldName = s.second;
        } else if (!s.first.IsNotFound()) {
            BS_LOG(ERROR, "get ttl_field_name from settings faild, status[%s]", s.first.ToString().c_str());
        }
    }
    BS_LOG(INFO, "ttl field name set as[%s]", _ttlFieldName.c_str());
    return true;
}

void DocTTLProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

bool DocTTLProcessor::process(const document::ExtendDocumentPtr& document)
{
    const document::RawDocumentPtr& rawDoc = document->getRawDocument();
    uint32_t docLiveTime = TimeUtility::currentTimeInSeconds() + _ttlTime;
    rawDoc->setField(_ttlFieldName, StringUtil::toString(docLiveTime));
    return true;
}

DocumentProcessor* DocTTLProcessor::clone() { return new DocTTLProcessor(*this); }

}} // namespace build_service::processor
