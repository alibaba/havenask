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

#include "indexlib/index/kv/KVMerger.h"

namespace indexlibv2::index {

class FixedLenKVMerger : public KVMerger
{
public:
    FixedLenKVMerger();
    ~FixedLenKVMerger();

private:
    Status Dump() override;
    KVTypeId MakeTypeId(const std::vector<SegmentStatistics>& statVec) const override;
    void FillSegmentMetrics(indexlib::framework::SegmentMetrics* segMetrics) override;
    bool NeedCompactBucket(const std::vector<SegmentStatistics>& statVec) const;

private:
    friend class FixedLenKVMergerTest_TestNeedCompactBucket_Test;
};

} // namespace indexlibv2::index
