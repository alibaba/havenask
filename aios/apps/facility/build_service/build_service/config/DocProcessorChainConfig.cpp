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
#include "build_service/config/DocProcessorChainConfig.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>

#include "alog/Logger.h"
#include "build_service/config/ConfigDefine.h"
#include "indexlib/base/Constant.h"

using namespace std;

namespace build_service { namespace config {

BS_LOG_SETUP(config, DocProcessorChainConfig);

void ProcessorInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("class_name", className);
    json.Jsonize("module_name", moduleName, string());
    json.Jsonize("parameters", parameters, KeyValueMap());
}

////////////////////////////////////////////////////////////////

DocProcessorChainConfig::DocProcessorChainConfig()
    : indexDocSerializeVersion(indexlib::INVALID_DOC_VERSION)
    , tolerateFormatError(true)
    , useRawDocAsDoc(false)
    , serializeRawDoc(false)
{
}

DocProcessorChainConfig::~DocProcessorChainConfig() {}

void DocProcessorChainConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("clusters", clusterNames);
    json.Jsonize("modules", moduleInfos, plugin::ModuleInfos());
    json.Jsonize("document_processor_chain", processorInfos, ProcessorInfos());
    json.Jsonize("sub_document_processor_chain", subProcessorInfos, ProcessorInfos());
    json.Jsonize("tolerate_field_format_error", tolerateFormatError, tolerateFormatError);
    json.Jsonize("use_raw_doc_as_document", useRawDocAsDoc, useRawDocAsDoc);
    json.Jsonize("serialize_raw_document", serializeRawDoc, serializeRawDoc);
    json.Jsonize("index_document_serialize_version", indexDocSerializeVersion, indexDocSerializeVersion);
    json.Jsonize("chain_name", chainName, chainName);
}

bool DocProcessorChainConfig::validate() const
{
    // hard to validate everything ...
    for (size_t i = 0; i < subProcessorInfos.size(); ++i) {
        string className = subProcessorInfos[i].className;
        if (className == MODIFIED_FIELDS_DOCUMENT_PROCESSOR_NAME) {
            string errorMsg = "sub document processor chain not support"
                              "ModifiedFieldsDocumentProcessor";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }

    if (serializeRawDoc && !useRawDocAsDoc) {
        BS_LOG(ERROR, "do not support serialize_raw_document=true and use_raw_doc_as_document=false");
        return false;
    }
    return true;
}

}} // namespace build_service::config
