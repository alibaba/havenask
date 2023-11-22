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

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/index/normal/accessor/group_field_data_writer.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader.h"
#include "indexlib/index/normal/summary/summary_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

DECLARE_REFERENCE_CLASS(util, MMapAllocator);
namespace indexlib { namespace index {

class LocalDiskSummaryWriter
{
public:
    LocalDiskSummaryWriter() {}

    ~LocalDiskSummaryWriter() {}

public:
    void Init(const config::SummaryGroupConfigPtr& summaryGroupConfig,
              util::BuildResourceMetrics* buildResourceMetrics);
    void AddDocument(const autil::StringView& data);
    void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool,
              const std::string& temperatureLayer);
    const SummarySegmentReaderPtr CreateInMemSegmentReader();
    uint32_t GetNumDocuments() const { return mDataWriter->GetNumDocuments(); }
    const std::string& GetGroupName() const { return mSummaryGroupConfig->GetGroupName(); }

private:
    config::SummaryGroupConfigPtr mSummaryGroupConfig;
    GroupFieldDataWriterPtr mDataWriter;

private:
    friend class SummaryWriterTest;
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<LocalDiskSummaryWriter> LocalDiskSummaryWriterPtr;
}} // namespace indexlib::index
