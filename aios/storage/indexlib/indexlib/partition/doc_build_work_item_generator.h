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

#include "autil/Lock.h"
#include "autil/ThreadPool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/partition_writer.h"

DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(config, SourceSchema);
DECLARE_REFERENCE_CLASS(config, SummarySchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, DocumentCollector);
DECLARE_REFERENCE_CLASS(index, BuildWorkItem);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(partition, SingleSegmentWriter);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, InplaceModifier);
DECLARE_REFERENCE_CLASS(util, GroupedThreadPool);

namespace indexlib { namespace partition {

// DocBuildWorkItemGenerator负责batch build中对某一个DocumentCollector，生成分字段可以并行构建的work
// item。支持正排、倒排、summary、source类型，支持主子表场景。其中，为了减少长尾，倒排类型是按照字段shard粒度生成work
// item，其余类型是按照字段粒度生成work item。 另外，batch build中为了实现不同batch(批次)
// 之间的流水线构造，不会生成PK字段对应的work item。PK字段会放在主线程执行。详情参考TODO。

class DocBuildWorkItemGenerator
{
public:
    DocBuildWorkItemGenerator() = delete;
    ~DocBuildWorkItemGenerator() = delete;

public:
    // Attention: @param documents 中的 Document 内容会被修改，外部不要依赖其继续使用
    static void Generate(PartitionWriter::BuildMode buildMode, const config::IndexPartitionSchemaPtr& schema,
                         const index_base::SegmentWriterPtr& segmentWriter,
                         const partition::PartitionModifierPtr& modifier,
                         const std::shared_ptr<document::DocumentCollector>& docCollector,
                         util::GroupedThreadPool* threadPool);

private:
    static void DoGenerate(PartitionWriter::BuildMode buildMode, const config::IndexPartitionSchemaPtr& schema,
                           const partition::SingleSegmentWriterPtr& segmentWriter,
                           const partition::InplaceModifierPtr& modifier, bool isSub,
                           const std::shared_ptr<document::DocumentCollector>& docCollector,
                           util::GroupedThreadPool* threadPool);

    static void CreateIndexWorkItem(PartitionWriter::BuildMode buildMode,
                                    const std::shared_ptr<document::DocumentCollector>& docCollector,
                                    const partition::SingleSegmentWriterPtr& segmentWriter,
                                    const partition::InplaceModifierPtr& modifier,
                                    const config::IndexSchemaPtr& indexSchema, bool isSub,
                                    util::GroupedThreadPool* threadPool);
    static void CreateAttributeWorkItem(PartitionWriter::BuildMode buildMode,
                                        const std::shared_ptr<document::DocumentCollector>& docCollector,
                                        const partition::SingleSegmentWriterPtr& segmentWriter,
                                        const partition::InplaceModifierPtr& modifier,
                                        const config::AttributeSchemaPtr& attrSchema, bool isVirtual, bool isSub,
                                        util::GroupedThreadPool* threadPool);
    static void CreateSourceWorkItem(const std::shared_ptr<document::DocumentCollector>& docCollector,
                                     const partition::SingleSegmentWriterPtr& segmentWriter,
                                     const config::SourceSchemaPtr& sourceSchema, bool isSub,
                                     util::GroupedThreadPool* threadPool);
    static void CreateSummaryWorkItem(const std::shared_ptr<document::DocumentCollector>& docCollector,
                                      const partition::SingleSegmentWriterPtr& segmentWriter,
                                      const config::SummarySchemaPtr& summarySchema, bool isSub,
                                      util::GroupedThreadPool* threadPool);

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::partition
