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
#ifndef __INDEXLIB_REOPEN_DECIDER_H
#define __INDEXLIB_REOPEN_DECIDER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/online_config.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/partition_writer.h"

DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

namespace indexlib { namespace partition {

class ReopenDecider
{
public:
    enum ReopenType {
        NORMAL_REOPEN,
        RECLAIM_READER_REOPEN,
        SWITCH_RT_SEGMENT_REOPEN,
        FORCE_REOPEN,
        NO_NEED_REOPEN,
        NEED_FORCE_REOPEN,
        UNABLE_NORMAL_REOPEN,
        UNABLE_FORCE_REOPEN,
        UNABLE_REOPEN,
        INVALID_REOPEN,
        INCONSISTENT_SCHEMA_REOPEN,
        INDEX_ROLL_BACK_REOPEN,
    };

public:
    ReopenDecider(const config::OnlineConfig& onlineConfig, bool forceReopen);

    virtual ~ReopenDecider();

public:
    void Init(const index_base::PartitionDataPtr& partitionData, const std::string& indexSourceRoot,
              const index::AttributeMetrics* attributeMetrics, int64_t maxRecoverTs,
              versionid_t reopenVersionId = INVALID_VERSION,
              const OnlinePartitionReaderPtr& onlineReader = OnlinePartitionReaderPtr());

    virtual ReopenType GetReopenType() const { return mReopenType; }

    virtual index_base::Version GetReopenIncVersion() const { return mReopenIncVersion; }

    virtual void ValidateDeploySegments(const index_base::PartitionDataPtr& partitionData,
                                        const index_base::Version& version) const;

private:
    bool NeedReclaimReaderMem(const index::AttributeMetrics* attributeMetrics) const;

    bool NeedSwitchFlushRtSegments(const OnlinePartitionReaderPtr& onlineReader,
                                   const index_base::PartitionDataPtr& partitionData);

protected:
    // virtual for test
    virtual bool GetNewOnDiskVersion(const index_base::PartitionDataPtr& partitionData,
                                     const std::string& indexSourceRoot, versionid_t reopenVersionId,
                                     index_base::Version& version) const;

private:
    config::OnlineConfig mOnlineConfig;
    index_base::Version mReopenIncVersion;
    bool mForceReopen;
    ReopenType mReopenType;

private:
    friend class ReopenDeciderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReopenDecider);
}} // namespace indexlib::partition

#endif //__INDEXLIB_REOPEN_DECIDER_H
