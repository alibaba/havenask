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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(util, QuotaControl);
DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);

namespace indexlib { namespace index {

class KvResourceAssigner
{
public:
    KvResourceAssigner(const config::KVIndexConfigPtr& kvIndexConfig, uint32_t columnIdx, uint32_t totalColumnCount);
    ~KvResourceAssigner();

public:
    void Init(const util::QuotaControlPtr& buildMemoryQuotaControler, const config::IndexPartitionOptions& options);

    size_t Assign(const std::shared_ptr<framework::SegmentMetrics>& metrics);

private:
    void GetDataRatio(const std::shared_ptr<framework::SegmentMetrics>& metrics, double& curMemoryRatio);

private:
    size_t mTotalMemQuota;
    config::KVIndexConfigPtr mKvIndexConfig;
    uint32_t mColumnIdx;
    uint32_t mColumnCount;

private:
    static const uint64_t MAX_MEMORY_USE_RESERVE_MIN = 1024; // 1K
    static constexpr double MAX_MEMORY_USE_RESERVE_RATIO = 0.01;
    static constexpr double DEFAULT_BALANCE_STRATEGY_RATIO = 0.3;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KvResourceAssigner);
}} // namespace indexlib::index
