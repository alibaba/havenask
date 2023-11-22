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
#include "build_service/processor/EncodingConvertDocumentProcessor.h"

#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <ostream>
#include <utility>

#include "alog/Logger.h"
#include "autil/ConstString.h"
#include "autil/Span.h"
#include "autil/StringTokenizer.h"
#include "build_service/document/ClassifiedDocument.h"
#include "indexlib/document/RawDocument.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, EncodingConvertDocumentProcessor);

const string EncodingConvertDocumentProcessor::PROCESSOR_NAME = string("EncodingConvertDocumentProcessor");
const string EncodingConvertDocumentProcessor::SRC_ENCODING = string("src_encoding");
const string EncodingConvertDocumentProcessor::DST_ENCODING = string("dst_encoding");
const string EncodingConvertDocumentProcessor::TO_CONVERT_FIELDS = string("to_convert_fields");
const string EncodingConvertDocumentProcessor::IGNORE_ERROR = string("ignore_error");
const string EncodingConvertDocumentProcessor::FIELD_NAME_SEP = string(";");

EncodingConvertDocumentProcessor::EncodingConvertDocumentProcessor() { _iconvDesc = (iconv_t)-1; }

EncodingConvertDocumentProcessor::EncodingConvertDocumentProcessor(const EncodingConvertDocumentProcessor& other)
{
    _srcEncoding = other._srcEncoding;
    _dstEncoding = other._dstEncoding;
    _toConvertFieldNames = other._toConvertFieldNames;
    _iconvDesc = (iconv_t)-1;
}

EncodingConvertDocumentProcessor::~EncodingConvertDocumentProcessor()
{
    if (_iconvDesc != (iconv_t)-1) {
        iconv_close(_iconvDesc);
    }
}

bool EncodingConvertDocumentProcessor::init(const DocProcessorInitParam& param)
{
    const KeyValueMap& kvMap = param.parameters;
    _toConvertFieldNames.clear();
    bool ignoreError = true;

    KeyValueMap::const_iterator it = kvMap.find(IGNORE_ERROR);
    if (it != kvMap.end() && it->second == "false") {
        ignoreError = false;
    }

    it = kvMap.find(SRC_ENCODING);
    if (it == kvMap.end()) {
        string errorMsg = "src_encoding is not configured";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _srcEncoding = it->second;

    it = kvMap.find(DST_ENCODING);
    if (it == kvMap.end()) {
        string errorMsg = "dst_encoding is not configured";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _dstEncoding = it->second;
    if (ignoreError) {
        _dstEncoding += "//IGNORE";
    }

    it = kvMap.find(TO_CONVERT_FIELDS);
    if (it == kvMap.end()) {
        string errorMsg = "to_convert_fields is not configured";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    StringTokenizer tokenizer(it->second, FIELD_NAME_SEP,
                              StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = tokenizer.begin(); it != tokenizer.end(); it++) {
        _toConvertFieldNames.push_back(*it);
    }
    return initIconv();
}

bool EncodingConvertDocumentProcessor::initIconv()
{
    _iconvDesc = iconv_open(_dstEncoding.c_str(), _srcEncoding.c_str());
    if (_iconvDesc == (iconv_t)-1) {
        string errorMsg = "iconv_open failed, src_encoding:[" + _srcEncoding + "], dst_encoding:[" + _dstEncoding + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool EncodingConvertDocumentProcessor::process(const ExtendDocumentPtr& document)
{
    if (_iconvDesc == (iconv_t)-1 && !initIconv()) {
        return false;
    }
    RawDocumentPtr rawDoc = document->getRawDocument();
    for (FieldNameVector::const_iterator it = _toConvertFieldNames.begin(); it != _toConvertFieldNames.end(); ++it) {
        processOneField(rawDoc, *it);
    }

    return true;
}

void EncodingConvertDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void EncodingConvertDocumentProcessor::processOneField(const RawDocumentPtr& rawDoc, const string& fieldName)
{
    StringView name(fieldName);
    const StringView& fieldValue = rawDoc->getField(name);
    if (fieldValue.empty()) {
        return;
    }
    char* src = const_cast<char*>(fieldValue.data());
    char buffer[MAX_BUF_SIZE];
    char* dst = buffer;
    size_t srcLen = fieldValue.size();
    size_t dstLen = MAX_BUF_SIZE;
    size_t retValue = iconv(_iconvDesc, &src, &srcLen, &dst, &dstLen);
    if (retValue == (size_t)-1) {
        stringstream ss;
        ss << "failed to iconv: " << fieldValue;
        string errorMsg = ss.str();
        BS_LOG(WARN, "%s", errorMsg.c_str());
    }
    *dst = '\0';
    StringView value = autil::MakeCString(buffer, dst - buffer, rawDoc->getPool());
    rawDoc->setFieldNoCopy(name, value);
}

void EncodingConvertDocumentProcessor::destroy() { delete this; }

DocumentProcessor* EncodingConvertDocumentProcessor::clone() { return new EncodingConvertDocumentProcessor(*this); }

}} // namespace build_service::processor
