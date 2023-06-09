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

class ValueWriter;

class VarLenKVMerger : public KVMerger
{
public:
    VarLenKVMerger();
    ~VarLenKVMerger();

private:
    Status PrepareMerge(const SegmentMergeInfos& segMergeInfos) override;
    Status AddRecord(const Record& record) override;
    Status Dump() override;
    std::pair<int64_t, int64_t> EstimateMemoryUsage(const std::vector<SegmentStatistics>& statVec) const override;
    void FillSegmentMetrics(indexlib::framework::SegmentMetrics* segMetrics) override;

private:
    Status CreateValueWriter();

private:
    std::unique_ptr<ValueWriter> _valueWriter;
};

} // namespace indexlibv2::index
