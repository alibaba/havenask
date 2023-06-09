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

#include <stddef.h>

#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "suez/sdk/TableReader.h"

namespace build_service::util {
class SwiftClientCreator;
} // namespace build_service::util

namespace build_service::workflow {
class RealtimeInfoWrapper;
} // namespace build_service::workflow

namespace suez {

struct PartitionId;

struct PartitionSourceReaderConfig {
    suez::PartitionId pid;
    build_service::workflow::RealtimeInfoWrapper realtimeInfo;
};

class SourceReaderProvider {
public:
    typedef std::map<suez::PartitionId, PartitionSourceReaderConfig> SourceConfigMap;

public:
    SourceReaderProvider();
    ~SourceReaderProvider();

public:
    void erase(const PartitionId &pid);
    void clear();
    bool addSourceConfig(PartitionSourceReaderConfig config);
    const PartitionSourceReaderConfig *getSourceConfigByIdx(const std::string &tableName, int32_t index) const;

public:
    std::shared_ptr<build_service::util::SwiftClientCreator> swiftClientCreator;
    std::map<std::string, SourceConfigMap> sourceConfigs;
};

} // namespace suez
