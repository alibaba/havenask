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
#include "ha3/qrs/QrsProcessor.h"

#include <assert.h>
#include <cstddef>
#include <map>
#include <string>
#include <utility>

#include "ha3/common/ClauseBase.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/isearch.h"
#include "indexlib/index_base/schema_adapter.h"
#include "autil/Log.h"

using namespace std;
using namespace isearch::common;

namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, QrsProcessor);

QrsProcessor::QrsProcessor()
    : _tracer(nullptr)
    , _timeoutTerminator(nullptr)
    , _ha3BizMeta(nullptr)
{
}

QrsProcessor::QrsProcessor(const QrsProcessor &processor) {
    _keyValues = processor._keyValues;
    _nextProcessor.reset();
    _tracer = NULL;
    _timeoutTerminator = NULL;
}

QrsProcessor::~QrsProcessor() {
}

void QrsProcessor::process(RequestPtr &requestPtr,
                           ResultPtr &resultPtr)
{
    REQUEST_TRACE(DEBUG, "begin %s process", getName().c_str());
    assert(resultPtr);
    if (resultPtr->hasError()) {
        return;
    }
    if(_nextProcessor.get()) {
        _nextProcessor->process(requestPtr, resultPtr);
    }
}

bool QrsProcessor::init(const QrsProcessorInitParam &param) {
    return init(*param.keyValues, param.resourceReader);
}

bool QrsProcessor::init(const KeyValueMap &keyValues,
                        config::ResourceReader *resourceReader)
{
    _keyValues = keyValues;
    return true;
}

QrsProcessorPtr QrsProcessor::getNextProcessor()
{
    return _nextProcessor;
}

void QrsProcessor::setNextProcessor(QrsProcessorPtr processor) {
    _nextProcessor = processor;
}

void QrsProcessor::setTimeoutTerminator(common::TimeoutTerminator *timeoutTerminator) {
    _timeoutTerminator = timeoutTerminator;
    if (_nextProcessor) {
        _nextProcessor->setTimeoutTerminator(timeoutTerminator);
    }
}

string QrsProcessor::getName() const {
    return "QrsProcessor";
}

const KeyValueMap& QrsProcessor::getParams() const {
    return _keyValues;
}

string QrsProcessor::getParam(const string &key) const {
    KeyValueMap::const_iterator it = _keyValues.find(key);
    if (it != _keyValues.end()) {
        return it->second;
    }
    return "";
}

void QrsProcessor::fillSummary(const RequestPtr &requestPtr,
                               const ResultPtr &resultPtr)
{
    REQUEST_TRACE(DEBUG, "begin %s fillSummary", getName().c_str());
    if(_nextProcessor) {
        _nextProcessor->fillSummary(requestPtr, resultPtr);
    }
}

std::unique_ptr<indexlibv2::config::ITabletSchema>
QrsProcessor::loadSchema(const std::string &schemaFilePath,
                         config::ResourceReader *resourceReader) {
    if (!resourceReader) {
        AUTIL_LOG(ERROR, "resourceReader is nullptr");
        return nullptr;
    }
    std::string schemaStr;    
    if (!resourceReader->getFileContent(schemaStr, schemaFilePath)) {
        AUTIL_LOG(ERROR, "read schema file [%s] failed", schemaFilePath.c_str());
        return nullptr;
    }
    return indexlib::index_base::SchemaAdapter::LoadSchema(schemaStr);
}

} // namespace qrs
} // namespace isearch
