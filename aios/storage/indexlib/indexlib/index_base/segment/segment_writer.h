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

#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index_base/index_meta/segment_metrics_util.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Status.h"
#include "indexlib/util/memory_control/BuildResourceCalculator.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegmentReader);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, SegmentTemperatureMeta);
DECLARE_REFERENCE_CLASS(partition, InMemorySegmentModifier);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, QuotaControl);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(common, DumpItem);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, KVIndexDocument);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace index_base {

class SegmentWriter
{
public:
    SegmentWriter(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                  uint32_t columnIdx = 0, uint32_t totalColumnCount = 1)
        : mSchema(schema)
        , mOptions(options)
        , mColumnIdx(columnIdx)
        , mTotalColumnCount(totalColumnCount)
        , mStatus(util::UNKNOWN)
    {
    }

    virtual ~SegmentWriter() {}

public:
    // for kv & kkv init
    virtual void Init(BuildingSegmentData& segmentData,
                      const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics,
                      const util::QuotaControlPtr& buildMemoryQuotaControler,
                      const util::BuildResourceMetricsPtr& buildResourceMetrics = util::BuildResourceMetricsPtr())
    {
        assert(false);
    }

    virtual void CollectSegmentMetrics() = 0;
    // for index table
    virtual void Init(const SegmentData& segmentData,
                      const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics,
                      const util::BuildResourceMetricsPtr& buildResourceMetrics, const util::CounterMapPtr& counterMap,
                      const plugin::PluginManagerPtr& pluginManager,
                      const PartitionSegmentIteratorPtr& partSegIter = PartitionSegmentIteratorPtr())
    {
        assert(false);
    }

    virtual SegmentWriter* CloneWithNewSegmentData(BuildingSegmentData& segmentData, bool isShared) const = 0;

    virtual bool AddDocument(const document::DocumentPtr& doc) = 0;
    virtual bool IsDirty() const = 0;
    virtual void EndSegment() = 0;
    virtual void CreateDumpItems(const file_system::DirectoryPtr& directory,
                                 std::vector<std::unique_ptr<common::DumpItem>>& dumpItems) = 0;
    virtual InMemorySegmentReaderPtr CreateInMemSegmentReader() = 0;
    virtual size_t GetTotalMemoryUse() const;
    virtual const SegmentInfoPtr& GetSegmentInfo() const = 0;
    virtual bool GetSegmentTemperatureMeta(index_base::SegmentTemperatureMeta& temperatureMeta)
    {
        assert(false);
        return false;
    }
    virtual bool HasCustomizeMetrics(std::shared_ptr<indexlib::framework::SegmentGroupMetrics>& customizeMetrics) const
    {
        return false;
    }
    virtual partition::InMemorySegmentModifierPtr GetInMemorySegmentModifier() = 0;
    virtual bool NeedForceDump() { return false; }
    virtual util::Status GetStatus() const { return mStatus; }
    virtual bool AddKVIndexDocument(document::KVIndexDocument* doc)
    {
        assert(false);
        return false;
    }
    const util::CounterMapPtr& GetCounterMap() const { return mCounterMap; }
    const util::BuildResourceMetricsPtr& GetBuildResourceMetrics() const { return mBuildResourceMetrics; }

    bool IsShardingColumn() const { return mTotalColumnCount > 1; }
    uint32_t GetColumnIdx() const { return mColumnIdx; }
    uint32_t GetTotalColumnCount() const { return mTotalColumnCount; }
    const std::shared_ptr<indexlib::framework::SegmentMetrics>& GetSegmentMetrics() const { return mSegmentMetrics; }

    static std::string GetMetricsGroupName(uint32_t columnIdx)
    {
        return SegmentMetricsUtil::GetColumnGroupName(columnIdx);
    }

    const std::shared_ptr<config::FileCompressSchema>& GetFileCompressSchema() const
    {
        return mSchema->GetFileCompressSchema();
    }

protected:
    void SetStatus(util::Status status) { mStatus = status; }

protected:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    util::BuildResourceMetricsPtr mBuildResourceMetrics;
    uint32_t mColumnIdx;
    uint32_t mTotalColumnCount;
    volatile util::Status mStatus;
    util::CounterMapPtr mCounterMap;
    std::shared_ptr<indexlib::framework::SegmentMetrics> mSegmentMetrics;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentWriter);

///////////////////////////////////////////////////////////////////
inline size_t SegmentWriter::GetTotalMemoryUse() const
{
    assert(mBuildResourceMetrics);
    return util::BuildResourceCalculator::GetCurrentTotalMemoryUse(mBuildResourceMetrics);
}
}} // namespace indexlib::index_base
