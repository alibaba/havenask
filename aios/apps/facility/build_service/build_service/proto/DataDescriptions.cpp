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
#include "build_service/proto/DataDescriptions.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/CLIOptionNames.h"

using namespace std;
using namespace autil;
using namespace build_service::config;

namespace build_service { namespace proto {
BS_LOG_SETUP(config, DataDescriptions);

DataDescriptions::DataDescriptions() {}

DataDescriptions::~DataDescriptions() {}

bool DataDescriptions::validate() const
{
    set<string> srcs;
    for (size_t i = 0; i < _dataDescriptions.size(); i++) {
        const DataDescription& dataDescription = _dataDescriptions[i];
        string src = getValueFromKeyValueMap(dataDescription.kvMap, READ_SRC);
        string type = getValueFromKeyValueMap(dataDescription.kvMap, READ_SRC_TYPE);
        if (src.empty() || type.empty()) {
            string errorMsg =
                "data description[" + autil::legacy::ToJsonString(dataDescription) + "] must have src and type";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        if (srcs.end() != srcs.find(src)) {
            BS_LOG(INFO, "multi src [%s] in data description", src.c_str());
            return false;
        }

        string fullProcessorCountStr = getValueFromKeyValueMap(dataDescription.kvMap, "full_processor_count");
        if (!fullProcessorCountStr.empty()) {
            uint32_t fullProcessorCount = 0;
            if (!StringUtil::fromString(fullProcessorCountStr, fullProcessorCount)) {
                BS_LOG(ERROR, "invalid full_processor_count[%s], only integer is allowed",
                       fullProcessorCountStr.c_str());
                return false;
            }
            if (fullProcessorCount == 0) {
                BS_LOG(ERROR, "invalid full_processor_count[%s], only positive integer is allowed",
                       fullProcessorCountStr.c_str());
                return false;
            }
        }
        srcs.insert(src);
    }
    return true;
}

}} // namespace build_service::proto
