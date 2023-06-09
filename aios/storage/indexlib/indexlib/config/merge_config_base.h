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
#ifndef __INDEXLIB_MERGE_CONFIG_BASE_H
#define __INDEXLIB_MERGE_CONFIG_BASE_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class MergeConfigBase : public autil::legacy::Jsonizable
{
public:
#if _GLIBCXX_USE_CXX11_ABI
    static_assert(sizeof(autil::legacy::Jsonizable) == 8, "autil::legacy::Jsonizable size change!");
    static_assert(sizeof(std::string) == 32, "string size change!");
#else
    static_assert(sizeof(autil::legacy::Jsonizable) == 8, "autil::legacy::Jsonizable size change!");
    static_assert(sizeof(std::string) == 8, "string size change!");
#endif

public:
    MergeConfigBase();
    ~MergeConfigBase();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

public:
    // attention: do not add new member to this class
    // U can add new member to MergeConfigImpl
    std::string mergeStrategyStr;       // 8B in old abi, 32 in new abi
    std::string docReclaimConfigPath;   // 8B in old abi, 32 in new abi
    int64_t maxMemUseForMerge;          // 8B
    uint32_t mergeThreadCount;          // 4B
    size_t uniqPackAttrMergeBufferSize; // 8B

    config::MergeIOConfig mergeIOConfig;                  // 40B
    config::TruncateOptionConfigPtr truncateOptionConfig; // 16B
    MergeStrategyParameter mergeStrategyParameter;        // 32B
    bool enableMergeItemCheckPoint;                       // 1B
    bool enableReclaimExpiredDoc;                         // 1B

public:
    static const int64_t DEFAULT_MAX_MEMORY_USE_FOR_MERGE = 40 * 1024; // 40GB
    static const uint32_t DEFAULT_MERGE_THREAD_COUNT = 3;
    static const uint32_t MAX_MERGE_THREAD_COUNT = 20;
    static const std::string DEFAULT_MERGE_STRATEGY;
    static const size_t DEFAULT_UNIQ_ENCODE_PACK_ATTR_MERGE_BUFFER_SIZE = 64; // 64 MB

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeConfigBase);
}} // namespace indexlib::config

#endif //__INDEXLIB_MERGE_CONFIG_BASE_H
