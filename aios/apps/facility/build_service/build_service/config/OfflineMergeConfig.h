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
#ifndef ISEARCH_BS_OFFLINEMERGECONFIG_H
#define ISEARCH_BS_OFFLINEMERGECONFIG_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/MergePluginConfig.h"
#include "build_service/util/Log.h"
#include "indexlib/config/merge_config.h"

namespace build_service { namespace config {

class OfflineMergeConfig : public autil::legacy::Jsonizable
{
public:
    OfflineMergeConfig();
    ~OfflineMergeConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    indexlib::config::MergeConfig mergeConfig;
    std::string periodMergeDescription;
    uint32_t mergeParallelNum;
    uint32_t mergeSleepTime; // for test only
    MergePluginConfig mergePluginConfig;
    bool needWaitAlterField;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(OfflineMergeConfig);

}} // namespace build_service::config

#endif // ISEARCH_BS_OFFLINEMERGECONFIG_H
