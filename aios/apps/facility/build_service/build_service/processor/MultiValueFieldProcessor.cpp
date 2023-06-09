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
#include "build_service/processor/MultiValueFieldProcessor.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace autil::legacy;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace build_service::analyzer;
using namespace build_service::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, MultiValueFieldProcessor);

const string MultiValueFieldProcessor::PROCESSOR_NAME = "MultiValueFieldProcessor";

MultiValueFieldProcessor::MultiValueFieldProcessor() {}

MultiValueFieldProcessor::~MultiValueFieldProcessor() {}

bool MultiValueFieldProcessor::init(const DocProcessorInitParam& param)
{
    // json string: {
    //   "field1" : ",",
    //   "field2" : ";"
    //}
    string fieldSepDesc = getValueFromKeyValueMap(param.parameters, "field_separator_description");
    BS_LOG(INFO, "fieldSepDesc: %s", fieldSepDesc.c_str());
    try {
        FromJsonString(_fieldSepDescription, fieldSepDesc);
    } catch (const autil::legacy::ExceptionBase& e) {
        return false;
    }
    return true;
}

bool MultiValueFieldProcessor::process(const ExtendDocumentPtr& document)
{
    assert(document);
    RawDocumentPtr rawDoc = document->getRawDocument();
    for (const auto& it : _fieldSepDescription) {
        const string& fieldName = it.first;
        const string& sep = it.second;
        string fieldValue = rawDoc->getField(fieldName);
        // replace
        auto fieldValueSlices = autil::StringUtil::split(fieldValue, sep);
        fieldValue = autil::StringUtil::toString(fieldValueSlices, string(1, MULTI_VALUE_SEPARATOR));
        rawDoc->setField(fieldName, fieldValue);
    }
    return true;
}

void MultiValueFieldProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void MultiValueFieldProcessor::destroy() { delete this; }

DocumentProcessor* MultiValueFieldProcessor::clone() { return new MultiValueFieldProcessor(*this); }

}} // namespace build_service::processor
