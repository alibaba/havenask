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
#pragma once

#include <stdint.h>
#include <string>

#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/DataDescriptions.h"

namespace build_service { namespace admin {

struct ProcessorInput {
public:
    ProcessorInput() : src(-1), offset(-1), batchMask(-1), fullBuildProcessLastSwiftSrc(true) {}
    ~ProcessorInput();
    bool parseInput(const KeyValueMap& kvMap);
    static bool isBatchMode(proto::DataDescriptions& dataDescriptions);
    bool isBatchMode() { return isBatchMode(dataDescriptions); }
    bool operator==(const ProcessorInput& other) const
    {
        return dataDescriptions == other.dataDescriptions && src == other.src && offset == other.offset &&
               batchMask == other.batchMask && fullBuildProcessLastSwiftSrc == other.fullBuildProcessLastSwiftSrc &&
               rawDocQueryString == other.rawDocQueryString;
    }
    bool supportUpdateCheckpoint();
    bool supportUpdatePartitionCount();

private:
    bool parseDataDescriptions(const std::string& dataDescriptionStr);
    bool fillDataDescriptions(const KeyValueMap& kvMap);

public:
    proto::DataDescriptions dataDescriptions;
    int64_t src;
    int64_t offset;
    int32_t batchMask;
    bool fullBuildProcessLastSwiftSrc;
    std::string startCheckpointName;
    std::string rawDocQueryString;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorInput);

}} // namespace build_service::admin
