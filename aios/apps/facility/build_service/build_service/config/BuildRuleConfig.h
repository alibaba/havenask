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
#ifndef ISEARCH_BS_BUILDRULECONFIG_H
#define ISEARCH_BS_BUILDRULECONFIG_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class BuildRuleConfig : public autil::legacy::Jsonizable
{
public:
    BuildRuleConfig();
    ~BuildRuleConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    bool validate() const;

public:
    uint32_t partitionCount;
    uint32_t buildParallelNum;
    uint32_t incBuildParallelNum;
    uint32_t mergeParallelNum;
    uint32_t mapReduceRatio;
    uint32_t partitionSplitNum;
    bool needPartition;
    bool alignVersion;
    bool disableIncParallelInstanceDir;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildRuleConfig);

}} // namespace build_service::config

#endif // ISEARCH_BS_BUILDRULECONFIG_H
