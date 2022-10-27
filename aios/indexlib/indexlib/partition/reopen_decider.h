#ifndef __INDEXLIB_REOPEN_DECIDER_H
#define __INDEXLIB_REOPEN_DECIDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/online_config.h"
#include "indexlib/partition/partition_writer.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/online_partition_reader.h"

DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

IE_NAMESPACE_BEGIN(partition);

class ReopenDecider
{
public:
    enum ReopenType
    {
        NORMAL_REOPEN,
        RECLAIM_READER_REOPEN,
        SWITCH_RT_SEGMENT_REOPEN,
        FORCE_REOPEN,
        NO_NEED_REOPEN,
        NEED_FORCE_REOPEN,
        UNABLE_NORMAL_REOPEN,
        UNABLE_FORCE_REOPEN,
        INVALID_REOPEN,
        INCONSISTENT_SCHEMA_REOPEN,
        INDEX_ROLL_BACK_REOPEN,
    };

public:
    ReopenDecider(const config::OnlineConfig& onlineConfig,
                  bool forceReopen);

    virtual ~ReopenDecider();

public:
    void Init(const index_base::PartitionDataPtr& partitionData,
              const index::AttributeMetrics* attributeMetrics,
              versionid_t reopenVersionId = INVALID_VERSION,
              const OnlinePartitionReaderPtr& onlineReader = OnlinePartitionReaderPtr());

    virtual ReopenType GetReopenType() const
    { return mReopenType; }

    virtual index_base::Version GetReopenIncVersion() const
    { return mReopenIncVersion; }

private:
    bool NeedReclaimReaderMem(
        const index::AttributeMetrics* attributeMetrics) const;
    
    bool NeedSwitchFlushRtSegments(const OnlinePartitionReaderPtr& onlineReader,
                                   const index_base::PartitionDataPtr& partitionData);
    
protected:
    // virtual for test
    virtual bool GetNewOnDiskVersion(
        const index_base::PartitionDataPtr& partitionData,
            versionid_t reopenVersionId, index_base::Version& version) const;
    
    virtual void ValidateDeploySegments(
        const file_system::DirectoryPtr& rootDir, const index_base::Version& version) const; 

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

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_REOPEN_DECIDER_H
