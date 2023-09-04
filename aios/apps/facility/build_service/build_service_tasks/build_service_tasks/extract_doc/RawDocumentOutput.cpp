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
#include "build_service_tasks/extract_doc/RawDocumentOutput.h"

#include "autil/ConstString.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/task_base/BuildInTaskFactory.h"
#include "build_service_tasks/factory/BuildServiceTaskFactory.h"
#include "swift/client/MessageInfo.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service;
using namespace build_service::config;
using namespace build_service::io;
using namespace build_service::task_base;
using namespace build_service::document;

namespace build_service_tasks {

BS_LOG_SETUP(build_service_tasks, RawDocumentOutput);

const std::string RawDocumentOutput::OUTPUT_TYPE = "RAW_DOCUMENT";

RawDocumentOutput::RawDocumentOutput(const TaskOutputConfig& outputConfig)
    : Output(outputConfig)
    , _isSwiftOutput(false)
    , _factory(new BuildInTaskFactory)
{
}

bool RawDocumentOutput::init(const KeyValueMap& params)
{
    auto kvMap = params;
    _hashField = getValueFromKeyValueMap(kvMap, "hash_field");
    kvMap.erase("hash_field");
    if (!_hashField.empty()) {
        _hashFunc = autil::HashFuncFactory::createHashFunc(getValueFromKeyValueMap(kvMap, "hash_mode"));
        if (!_hashFunc) {
            BS_LOG(ERROR, "create hash_mode for hash_field [%s] failed", _hashField.c_str());
            return false;
        }
        kvMap.erase("hash_mode");
    }
    auto innerConfig = _outputConfig;
    for (const auto& kv : kvMap) {
        innerConfig.addParameters(kv.first, kv.second);
    }
    auto innerType = getValueFromKeyValueMap(innerConfig.getParameters(), "inner_output_type");
    innerConfig.setType(innerType);

    if (innerType == build_service::io::SWIFT) {
        _isSwiftOutput = true;
    }
    auto useTaskFactoryStr = getValueFromKeyValueMap(innerConfig.getParameters(), "inner_output_use_task_factory");
    bool useTaskFactory = false;
    if (!useTaskFactoryStr.empty()) {
        if (!autil::StringUtil::parseTrueFalse(useTaskFactoryStr, useTaskFactory)) {
            BS_LOG(ERROR, "inner_output_use_task_factory value [%s] invalid", useTaskFactoryStr.c_str());
            return false;
        }
    }
    if (useTaskFactory) {
        _factory.reset(new BuildServiceTaskFactory);
    }
    auto creator = _factory->getOutputCreator(innerConfig);
    if (!creator) {
        BS_LOG(ERROR, "create output failed");
        return false;
    }
    _innerOutput = creator->create({});
    if (!_innerOutput) {
        BS_LOG(ERROR, "create output failed");
        return false;
    }
    return true;
}

bool RawDocumentOutput::write(autil::legacy::Any& any)
{
    string rawDocString;
    try {
        const DocFields& docFields = AnyCast<DocFields>(any);
        rawDocString = serializeHa3RawDoc(docFields);
        Any out;
        if (_isSwiftOutput) {
            swift::client::MessageInfo message;
            message.data = std::move(rawDocString);
            if (_hashFunc) {
                auto iter = docFields.find(_hashField);
                if (iter != docFields.end()) {
                    message.uint16Payload = _hashFunc->getHashId(iter->second);
                }
            }
            out = std::move(message);
        } else {
            out = StringView(rawDocString);
        }
        return _innerOutput->write(out);
    } catch (const BadAnyCast& e) {
        BS_LOG(ERROR, "Cast Error: %s", e.what());
        return false;
    }
    return true;
}

string RawDocumentOutput::serializeHa3RawDoc(const DocFields& kvMap)
{
    string rawDocStr;
    rawDocStr.reserve(1024);
    if (kvMap.empty()) {
        return rawDocStr;
    }
    rawDocStr += CMD_TAG + KEY_VALUE_EQUAL_SYMBOL + CMD_ADD + KEY_VALUE_SEPARATOR;
    for (const auto& kv : kvMap) {
        rawDocStr += kv.first;
        rawDocStr += KEY_VALUE_EQUAL_SYMBOL;
        rawDocStr += kv.second;
        rawDocStr += KEY_VALUE_SEPARATOR;
    }
    rawDocStr += CMD_SEPARATOR;
    return rawDocStr;
}

bool RawDocumentOutput::commit() { return _innerOutput->commit(); }

} // namespace build_service_tasks
