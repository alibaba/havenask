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
#ifndef __INDEXLIB_SUB_DOC_SEGMENT_WRITER_H
#define __INDEXLIB_SUB_DOC_SEGMENT_WRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(index, DefaultAttributeFieldAppender);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(partition, SingleSegmentWriter);

namespace indexlib { namespace partition {

class SubDocSegmentWriter : public index_base::SegmentWriter
{
private:
    using index_base::SegmentWriter::Init;

public:
    SubDocSegmentWriter(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options);

    ~SubDocSegmentWriter();

public:
    void Init(const index_base::SegmentData& segmentData, const std::shared_ptr<framework::SegmentMetrics>& metrics,
              const util::CounterMapPtr& counterMap, const plugin::PluginManagerPtr& pluginManager,
              const index_base::PartitionSegmentIteratorPtr& partSegIter = index_base::PartitionSegmentIteratorPtr());

    SubDocSegmentWriter* CloneWithNewSegmentData(index_base::BuildingSegmentData& segmentData,
                                                 bool isShared) const override;
    void CollectSegmentMetrics() override;
    bool AddDocument(const document::DocumentPtr& doc) override;
    void EndSegment() override;
    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         std::vector<std::unique_ptr<common::DumpItem>>& dumpItems) override;

    index_base::InMemorySegmentReaderPtr CreateInMemSegmentReader() override;

    bool IsDirty() const override;

    // TODO: return main sub doc segment info
    const index_base::SegmentInfoPtr& GetSegmentInfo() const override;

    InMemorySegmentModifierPtr GetInMemorySegmentModifier() override { return mMainModifier; }

    SingleSegmentWriterPtr GetMainWriter() const { return mMainWriter; }
    SingleSegmentWriterPtr GetSubWriter() const { return mSubWriter; }

public:
    static size_t EstimateInitMemUse(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                     const config::IndexPartitionSchemaPtr& schema,
                                     const config::IndexPartitionOptions& options,
                                     const plugin::PluginManagerPtr& pluginManager,
                                     const index_base::PartitionSegmentIteratorPtr& partSegIter);

private:
    SubDocSegmentWriter(const SubDocSegmentWriter& other, index_base::BuildingSegmentData& segmentData, bool isShared);

protected:
    SingleSegmentWriterPtr CreateSingleSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                                                     const config::IndexPartitionOptions& options,
                                                     const index_base::SegmentData& segmentData,
                                                     const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                     const plugin::PluginManagerPtr& pluginManager,
                                                     const index_base::PartitionSegmentIteratorPtr& partSegIter,
                                                     bool isSub);

protected:
    SingleSegmentWriterPtr mMainWriter;
    SingleSegmentWriterPtr mSubWriter;

    config::IndexPartitionSchemaPtr _subSchema;

    InMemorySegmentModifierPtr mMainModifier;

private:
    friend class SubDocSegmentWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocSegmentWriter);
}} // namespace indexlib::partition

#endif //__INDEXLIB_SUB_DOC_SEGMENT_WRITER_H
