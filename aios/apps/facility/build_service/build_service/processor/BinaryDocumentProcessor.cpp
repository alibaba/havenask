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
#include "build_service/processor/BinaryDocumentProcessor.h"

#include "autil/StringTokenizer.h"
#include "autil/legacy/base64.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace build_service::analyzer;
using namespace build_service::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, BinaryDocumentProcessor);

const string BinaryDocumentProcessor::PROCESSOR_NAME = "BinaryDocumentProcessor";
const string BinaryDocumentProcessor::BINARY_FIELD_NAMES = "binary_field_names";
const string BinaryDocumentProcessor::FIELD_NAME_SEP = ";";

BinaryDocumentProcessor::BinaryDocumentProcessor() {}

BinaryDocumentProcessor::~BinaryDocumentProcessor() {}

bool BinaryDocumentProcessor::init(const DocProcessorInitParam& param)
{
    const KeyValueMap& kvMap = param.parameters;
    _binaryFieldNames.clear();
    string fieldNames = getValueFromKeyValueMap(kvMap, BINARY_FIELD_NAMES);
    if (fieldNames.empty()) {
        BS_LOG(INFO, "binary field name is empty");
        return true;
    }

    StringTokenizer tokenizer(fieldNames, FIELD_NAME_SEP,
                              StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = tokenizer.begin(); it != tokenizer.end(); ++it) {
        _binaryFieldNames.push_back(*it);
        BS_LOG(INFO, "binary field name: [%s]", (*it).c_str());
    }
    return true;
}

bool BinaryDocumentProcessor::process(const ExtendDocumentPtr& document)
{
    assert(document);
    RawDocumentPtr rawDoc = document->getRawDocument();
    for (FieldNameVector::const_iterator it = _binaryFieldNames.begin(); it != _binaryFieldNames.end(); ++it) {
        processBinaryField(rawDoc, *it);
    }
    return true;
}

void BinaryDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void BinaryDocumentProcessor::destroy() { delete this; }

void BinaryDocumentProcessor::processBinaryField(const RawDocumentPtr& rawDoc, const string& fieldName)
{
    const string& fieldValue = rawDoc->getField(fieldName);
    if (fieldValue.empty()) {
        return;
    }
    string output;
    if (!base64Decode(fieldValue, output)) {
        string errorMsg = "failed to decode base64 field: [" + fieldName + "]";
        BS_LOG(WARN, "%s", errorMsg.c_str());
    } else {
        rawDoc->setField(fieldName, output);
    }
}

bool BinaryDocumentProcessor::base64Decode(const string& input, string& output)
{
    istringstream is(input);
    ostringstream os;

    try {
        autil::legacy::Base64Decoding(is, os);
    } catch (const ExceptionBase& e) {
        string errorMsg = "failed to decode base64 field, exception: " + string(e.what());
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    } catch (...) {
        string errorMsg = "failed to decode base64 field, exception unknown";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    output = os.str();
    return true;
}

DocumentProcessor* BinaryDocumentProcessor::clone() { return new BinaryDocumentProcessor(*this); }

}} // namespace build_service::processor
