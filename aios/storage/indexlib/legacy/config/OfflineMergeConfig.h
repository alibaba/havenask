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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/MergeConfig.h"

namespace indexlib::legacy::config {

class OfflineMergeConfig : public autil::legacy::Jsonizable
{
public:
    OfflineMergeConfig();
    ~OfflineMergeConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool NeedReclaimTask() const;

public:
    std::string periodMergeDescription;
    uint32_t mergeParallelNum;
    autil::legacy::json::JsonMap reclaimConfigs;
    autil::legacy::json::JsonMap mergeConfig;
    autil::legacy::json::JsonMap pluginConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::legacy::config
