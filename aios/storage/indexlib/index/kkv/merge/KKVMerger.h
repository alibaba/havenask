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

#include <any>
#include <map>
#include <memory>

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/common/KKVRecordFilter.h"
#include "indexlib/index/kkv/common/KKVSegmentStatistics.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/merge/OnDiskKKVIterator.h"
#include "indexlib/index/kkv/merge/OnDiskSinglePKeyIterator.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/metrics/ProgressMetrics.h"

namespace indexlibv2::index {

class KKVMerger : public IIndexMerger
{
public:
    KKVMerger() = default;
    ~KKVMerger() = default;

public:
    void SetStoreTs(bool storeTs) { _storeTs = storeTs; }

protected:
    static void FillSegmentMetrics(indexlib::framework::SegmentMetrics* segmentMetrics, const std::string& groupName,
                                   size_t pkeyCount, size_t skeyCount, size_t maxValueLen, size_t maxSkeyCount);

protected:
    bool _storeTs = false;
    indexlib::file_system::IOConfig _iOConfig;
    indexlib::util::ProgressMetricsPtr _mergeItemMetrics;
};

class KKVDataDumperBase;

template <typename SKeyType>
class KKVMergerTyped : public KKVMerger
{
public:
    using OnDiskSinglePKeyIteratorTyped = OnDiskSinglePKeyIterator<SKeyType>;
    using OnDiskKKVIteratorTyped = OnDiskKKVIterator<SKeyType>;

public:
    KKVMergerTyped() {}
    ~KKVMergerTyped() {}

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override;

private:
    Status CreateRecordFilter(const std::string& currentTime);
    Status PrepareMerge(const SegmentMergeInfos& segMergeInfos);
    Status DoMerge(const SegmentMergeInfos& segMergeInfos);
    StatusOr<std::shared_ptr<indexlib::file_system::IDirectory>>
    PrepareTargetSegmentDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& root);
    Status LoadSegmentStatistics(const SegmentMergeInfos& segMergeInfos,
                                 std::vector<KKVSegmentStatistics>& statVec) const;

private:
    Status CollectSinglePrefixKey(OnDiskSinglePKeyIteratorTyped* dataIter, bool isBottomLevel);
    void MoveToFirstValidSKeyPosition(OnDiskSinglePKeyIteratorTyped* dataIter, bool isBottomLevel);
    void MoveToNextValidSKeyPosition(OnDiskSinglePKeyIteratorTyped* dataIter, bool isBottomLevel);

private:
    bool _dropDeleteKey = false;
    int64_t _currentTsInSec = 0;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;

    indexlib::util::SimplePool _simplePool;
    std::unique_ptr<KKVRecordFilter> _recordFilter;
    std::shared_ptr<indexlib::file_system::IDirectory> _targetDir;
    std::shared_ptr<indexlib::file_system::IDirectory> _indexDir;

    std::shared_ptr<OnDiskKKVIteratorTyped> _iterator;
    std::shared_ptr<KKVDataDumperBase> _dataDumper;

    schemaid_t _schemaId = DEFAULT_SCHEMAID;

private:
    AUTIL_LOG_DECLARE();
};

MARK_EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(KKVMergerTyped);

} // namespace indexlibv2::index
