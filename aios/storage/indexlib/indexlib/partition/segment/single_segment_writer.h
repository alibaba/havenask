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
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index, InMemoryIndexSegmentWriter);
DECLARE_REFERENCE_CLASS(index, SegmentMetricsUpdater);
DECLARE_REFERENCE_CLASS(index, BuildProfilingMetrics);
DECLARE_REFERENCE_CLASS(index, InMemoryAttributeSegmentWriter);
DECLARE_REFERENCE_CLASS(index, SummaryWriter);
DECLARE_REFERENCE_CLASS(index, IndexWriter);
DECLARE_REFERENCE_CLASS(index, AttributeWriter);
DECLARE_REFERENCE_CLASS(index, PackAttributeWriter);
DECLARE_REFERENCE_CLASS(index, SourceWriter);
DECLARE_REFERENCE_CLASS(index, DeletionMapSegmentWriter);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

namespace indexlibv2::framework {
struct SegmentMeta;
}

namespace indexlib { namespace partition {

class SingleSegmentWriter : public index_base::SegmentWriter
{
public:
    SingleSegmentWriter(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                        bool isSub = false);

    ~SingleSegmentWriter();

public:
    void Init(const index_base::SegmentData& segmentData, const std::shared_ptr<framework::SegmentMetrics>& metrics,
              const util::BuildResourceMetricsPtr& buildResourceMetrics, const util::CounterMapPtr& counterMap,
              const plugin::PluginManagerPtr& pluginManager,
              const index_base::PartitionSegmentIteratorPtr& partSegIter =
                  index_base::PartitionSegmentIteratorPtr()) override;

    // void Open(indexlibv2::framework::SegmentMeta* segmentMeta, const util::CounterMapPtr& counterMap);

    SingleSegmentWriter* CloneWithNewSegmentData(index_base::BuildingSegmentData& segmentData,
                                                 bool isShared) const override;
    void CollectSegmentMetrics() override;

    bool AddDocument(const document::DocumentPtr& doc) override;
    void EndAddDocument(const document::DocumentPtr& doc);
    void EndSegment() override;
    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         std::vector<std::unique_ptr<common::DumpItem>>& dumpItems) override;

    index_base::InMemorySegmentReaderPtr CreateInMemSegmentReader() override;
    bool IsDirty() const override;

    const index_base::SegmentInfoPtr& GetSegmentInfo() const override { return mCurrentSegmentInfo; }
    InMemorySegmentModifierPtr GetInMemorySegmentModifier() override { return mModifier; }
    bool GetSegmentTemperatureMeta(index_base::SegmentTemperatureMeta& temperatureMeta) override;

    void SetBuildProfilingMetrics(const index::BuildProfilingMetricsPtr& metrics);

    bool HasCustomizeMetrics(std::shared_ptr<framework::SegmentGroupMetrics>& customizeMetrics) const override;

    // Needs to be called after Init.
    index::SummaryWriterPtr GetSummaryWriter() const;
    index::SourceWriterPtr GetSourceWriter() const;
    index::InMemoryIndexSegmentWriterPtr GetInMemoryIndexSegmentWriter() const;
    index::InMemoryAttributeSegmentWriterPtr GetInMemoryAttributeSegmentWriter(bool isVirtual) const
    {
        return isVirtual ? _virtualInMemoryAttributeSegmentWriter : _inMemoryAttributeSegmentWriter;
    }

public:
    // TODO: Remove this method.
    static size_t EstimateInitMemUse(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                     const config::IndexPartitionSchemaPtr& schema,
                                     const config::IndexPartitionOptions& options,
                                     const plugin::PluginManagerPtr& pluginManager,
                                     const index_base::PartitionSegmentIteratorPtr& partSegIter);

public:
    void TEST_SetSegmentInfo(const index_base::SegmentInfoPtr& segmentInfo) { mCurrentSegmentInfo = segmentInfo; }

private:
    void InitWriter(segmentid_t segmentId, const std::shared_ptr<framework::SegmentMetrics>& metrics,
                    const util::BuildResourceMetricsPtr& buildResourceMetrics, const util::CounterMapPtr& counterMap,
                    const plugin::PluginManagerPtr& pluginManager,
                    const index_base::PartitionSegmentIteratorPtr& partSegIter);

    bool IsValidDocument(const document::NormalDocumentPtr& doc) const;
    void CreateIndexDumpItems(const file_system::DirectoryPtr& directory,
                              std::vector<std::unique_ptr<common::DumpItem>>& dumpItems);
    void CreateAttributeDumpItems(const file_system::DirectoryPtr& directory,
                                  std::vector<std::unique_ptr<common::DumpItem>>& dumpItems);
    void CreateSummaryDumpItems(const file_system::DirectoryPtr& directory,
                                std::vector<std::unique_ptr<common::DumpItem>>& dumpItems);
    void CreateSourceDumpItems(const file_system::DirectoryPtr& directory,
                               std::vector<std::unique_ptr<common::DumpItem>>& dumpItems);

    void CreateDeletionMapDumpItems(const file_system::DirectoryPtr& directory,
                                    std::vector<std::unique_ptr<common::DumpItem>>& dumpItems);

    InMemorySegmentModifierPtr CreateInMemSegmentModifier() const;

private:
    bool mIsSub;
    index_base::SegmentData mSegmentData;
    index_base::SegmentInfoPtr mCurrentSegmentInfo;

    index::InMemoryIndexSegmentWriterPtr _inMemoryIndexSegmentWriter;
    index::InMemoryAttributeSegmentWriterPtr _inMemoryAttributeSegmentWriter;
    index::InMemoryAttributeSegmentWriterPtr _virtualInMemoryAttributeSegmentWriter;
    index::SummaryWriterPtr _summaryWriter;
    index::SourceWriterPtr _sourceWriter;
    index::DeletionMapSegmentWriterPtr mDeletionMapSegmentWriter;

    InMemorySegmentModifierPtr mModifier;
    index::SegmentMetricsUpdaterPtr mSegMetricsUpdater;
    index::BuildProfilingMetricsPtr mProfilingMetrics;
    indexlibv2::framework::SegmentMeta* mSegmentMeta;

private:
    friend class SingleSegmentWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleSegmentWriter);
}} // namespace indexlib::partition
