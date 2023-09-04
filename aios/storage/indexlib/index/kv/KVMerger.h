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
#include <unordered_set>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/SegmentStatistics.h"

namespace autil::mem_pool {
class PoolBase;
class UnsafePool;
} // namespace autil::mem_pool

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlibv2::framework {
class TabletData;
}

namespace indexlib::file_system {
class Directory;
}

namespace indexlib::framework {
class SegmentMetrics;
}

namespace indexlibv2::index {

struct Record;
class RecordFilter;
class KeyWriter;
class AdapterIgnoreFieldCalculator;

class KVMerger : public IIndexMerger
{
public:
    KVMerger();
    ~KVMerger();

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override;
    Status InitIgnoreFieldCalculater(framework::TabletData* tabletData);

protected:
    virtual KVTypeId MakeTypeId(const std::vector<SegmentStatistics>& statVec) const;
    virtual Status PrepareMerge(const SegmentMergeInfos& segMergeInfos);
    virtual Status AddRecord(const Record& record);
    virtual Status Dump() = 0;
    virtual void FillSegmentMetrics(indexlib::framework::SegmentMetrics* segMetrics) = 0;
    virtual std::pair<int64_t, int64_t> EstimateMemoryUsage(const std::vector<SegmentStatistics>& statVec);

private:
    Status CreateRecordFilter(const std::string& currentTime);
    // virtual for test
    virtual Status LoadSegmentStatistics(const SegmentMergeInfos& segMergeInfos,
                                         std::vector<SegmentStatistics>& statVec) const;
    StatusOr<std::shared_ptr<indexlib::file_system::Directory>>
    PrepareTargetSegmentDirectory(const std::shared_ptr<indexlib::file_system::Directory>& root) const;
    Status CreateKeyWriter(autil::mem_pool::PoolBase* pool, int64_t maxKeyMemoryUse,
                           std::vector<SegmentStatistics>& statVec);
    Status DoMerge(const SegmentMergeInfos& segMergeInfos);
    Status MergeMultiSegments(const SegmentMergeInfos& segMergeInfos);
    Status DeleteRecord(const Record& record);

public:
    bool TEST_GetDropDeleteKey() const { return _dropDeleteKey; }
    RecordFilter* TEST_GetRecordFilter() { return _recordFilter.get(); }

protected:
    KVTypeId _typeId;
    bool _dropDeleteKey = false;
    autil::mem_pool::UnsafePool _pool;
    std::unique_ptr<KeyWriter> _keyWriter;
    std::unique_ptr<RecordFilter> _recordFilter;
    std::unordered_set<keytype_t> _removedKeySet;
    std::shared_ptr<indexlib::file_system::Directory> _targetDir;
    std::shared_ptr<indexlibv2::config::KVIndexConfig> _indexConfig;
    std::shared_ptr<AdapterIgnoreFieldCalculator> _ignoreFieldCalculator;
    schemaid_t _schemaId = DEFAULT_SCHEMAID;
    std::unique_ptr<config::SortDescriptions> _sortDescriptions;
};

} // namespace indexlibv2::index
