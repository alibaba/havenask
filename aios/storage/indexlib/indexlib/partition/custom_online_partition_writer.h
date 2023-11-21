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
#include "indexlib/indexlib.h"
#include "indexlib/partition/custom_offline_partition_writer.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);
DECLARE_REFERENCE_CLASS(partition, PartitonData);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, OnlinePartitionMetrics);
DECLARE_REFERENCE_CLASS(document, Document);

namespace indexlib { namespace partition {

class CustomOnlinePartitionWriter : public CustomOfflinePartitionWriter
{
public:
    CustomOnlinePartitionWriter(const config::IndexPartitionOptions& options,
                                const table::TableFactoryWrapperPtr& tableFactory,
                                const FlushedLocatorContainerPtr& flushedLocatorContainer,
                                OnlinePartitionMetrics* onlinePartMetrics, const std::string& partitionName,
                                const PartitionRange& range = PartitionRange());
    ~CustomOnlinePartitionWriter();

public:
    void Open(const config::IndexPartitionSchemaPtr& schema, const index_base::PartitionDataPtr& partitionData,
              const PartitionModifierPtr& modifier) override;
    bool BuildDocument(const document::DocumentPtr& doc) override;
    void Close() override;
    void ReportMetrics() const override;
    bool NeedRewriteDocument(const document::DocumentPtr& doc) override { return false; }

private:
    bool CheckTimestamp(const document::DocumentPtr& doc) const;

private:
    OnlinePartitionMetrics* mOnlinePartMetrics;
    volatile bool mIsClosed;
    int64_t mRtFilterTimestamp;
    mutable autil::ThreadMutex mOnlineWriterLock;
    ;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomOnlinePartitionWriter);
}} // namespace indexlib::partition
