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
#include "indexlib/partition/partition_writer.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);

namespace indexlib { namespace partition {

// DocIdManager的主要功能是校验doc是否正确，和根据pk为doc分配doc id；这个类集中了对doc前置处理的大部分主要逻辑。
// 到2021-12，对于批式(batch)和流式(stream) 的构建，都统一通过DocIdManager分配doc
// id。
// (2021-12之前，分配doc id的逻辑分散在各个SegmentWriter和Modifier）
class DocIdManager
{
public:
    DocIdManager() {}
    virtual ~DocIdManager() {}

    DocIdManager(const DocIdManager&) = delete;
    DocIdManager& operator=(const DocIdManager&) = delete;
    DocIdManager(DocIdManager&&) = delete;
    DocIdManager& operator=(DocIdManager&&) = delete;

public:
    void Init(const index_base::PartitionDataPtr& partitionData, const PartitionModifierPtr& partitionModifier,
              const index_base::SegmentWriterPtr& segmentWriter, PartitionWriter::BuildMode buildMode,
              bool delayDedupDocument)
    {
        return Reinit(partitionData, partitionModifier, segmentWriter, buildMode, delayDedupDocument);
    }
    // use Reinit() to avoid re-contruct
    virtual void Reinit(const index_base::PartitionDataPtr& partitionData,
                        const PartitionModifierPtr& partitionModifier,
                        const index_base::SegmentWriterPtr& segmentWriter, PartitionWriter::BuildMode buildMode,
                        bool delayDedupDocument) = 0;
    virtual bool Process(const document::DocumentPtr& document) = 0;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::partition
