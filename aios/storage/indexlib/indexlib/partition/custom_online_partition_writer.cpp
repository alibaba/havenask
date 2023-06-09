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
#include "indexlib/partition/custom_online_partition_writer.h"

#include "indexlib/document/document.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/misc/log.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/table/table_factory_wrapper.h"

using namespace std;
using namespace autil;

using namespace indexlib::table;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, CustomOnlinePartitionWriter);

#define IE_LOG_PREFIX mPartitionName.c_str()

CustomOnlinePartitionWriter::CustomOnlinePartitionWriter(const IndexPartitionOptions& options,
                                                         const TableFactoryWrapperPtr& tableFactory,
                                                         const FlushedLocatorContainerPtr& flushedLocatorContainer,
                                                         OnlinePartitionMetrics* onlinePartMetrics,
                                                         const string& partitionName, const PartitionRange& range)
    : CustomOfflinePartitionWriter(options, tableFactory, flushedLocatorContainer,
                                   onlinePartMetrics->GetMetricProvider(), partitionName, range)
    , mOnlinePartMetrics(onlinePartMetrics)
    , mIsClosed(true)
{
}

CustomOnlinePartitionWriter::~CustomOnlinePartitionWriter() {}

void CustomOnlinePartitionWriter::Open(const config::IndexPartitionSchemaPtr& schema,
                                       const PartitionDataPtr& partitionData, const PartitionModifierPtr& modifier)
{
    ScopedLock lock(mOnlineWriterLock);
    Version incVersion = partitionData->GetOnDiskVersion();
    OnlineJoinPolicy joinPolicy(incVersion, schema->GetTableType(), document::SrcSignature());
    mRtFilterTimestamp = joinPolicy.GetRtFilterTimestamp();
    CustomOfflinePartitionWriter::Open(schema, partitionData, PartitionModifierPtr());
    mIsClosed = false;
}

bool CustomOnlinePartitionWriter::CheckTimestamp(const document::DocumentPtr& doc) const
{
    int64_t timestamp = doc->GetTimestamp();
    bool ret = false;
    ret = (timestamp >= mRtFilterTimestamp);
    if (!ret) {
        IE_PREFIX_LOG(DEBUG, "rt doc with ts[%ld], incTs[%ld], drop rt doc", timestamp, mRtFilterTimestamp);
        if (mOnlinePartMetrics) {
            mOnlinePartMetrics->IncreateObsoleteDocQps();
        }
    }
    return ret;
}

bool CustomOnlinePartitionWriter::BuildDocument(const DocumentPtr& doc)
{
    if (doc == nullptr) {
        IE_LOG(WARN, "empty document to build");
        ERROR_COLLECTOR_LOG(WARN, "empty document to build");
        return false;
    }
    if (mIsClosed) {
        IE_LOG(ERROR, "online writer is closed in partition[%s]", mPartitionName.c_str());
        mLastConsumedMessageCount = 0;
        return false;
    }
    // TODO: consider if table plugin need to customized filterTimestamp?
    // if (!CheckTimestamp(doc))
    // {
    //     return false;
    // }
    return CustomOfflinePartitionWriter::BuildDocument(doc);
}

void CustomOnlinePartitionWriter::Close()
{
    ScopedLock lock(mOnlineWriterLock);
    if (mIsClosed) {
        return;
    }
    // will trigger dumpSegment
    CustomOfflinePartitionWriter::DoClose();
    CustomOfflinePartitionWriter::Release();
    mIsClosed = true;
}

void CustomOnlinePartitionWriter::ReportMetrics() const
{
    if (mIsClosed) {
        return;
    }
    if (mOnlineWriterLock.trylock() == 0) {
        using UnlockerType = std::unique_ptr<autil::ThreadMutex, std::function<void(ThreadMutex*)>>;

        UnlockerType unlocker(&mOnlineWriterLock, [](ThreadMutex* lock) { lock->unlock(); });
        CustomOfflinePartitionWriter::ReportMetrics();
    }
}
}} // namespace indexlib::partition
