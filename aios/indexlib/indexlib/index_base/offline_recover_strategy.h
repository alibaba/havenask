#ifndef __INDEXLIB_OFFLINE_RECOVER_STRATEGY_H
#define __INDEXLIB_OFFLINE_RECOVER_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/configurator_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, Version);

IE_NAMESPACE_BEGIN(index_base);

class OfflineRecoverStrategy
{
public:
    OfflineRecoverStrategy();
    ~OfflineRecoverStrategy();

public:
    static index_base::Version Recover(index_base::Version version,
                                  const file_system::DirectoryPtr& physicalDir,
                                  config::RecoverStrategyType recoverType = config::RecoverStrategyType::RST_SEGMENT_LEVEL);

    static void RemoveLostSegments(index_base::Version version,
                                   const file_system::DirectoryPtr& physicalDir);
    
private:
    typedef std::pair<segmentid_t, std::string> SegmentInfoPair;
private:
    static void GetLostSegments(const file_system::DirectoryPtr& directory,
                                const index_base::Version& version,
                                segmentid_t lastSegmentId,
                                std::vector<SegmentInfoPair>& lostSegments);

    static void CleanUselessSegments(
            const std::vector<SegmentInfoPair>& lostSegments,
            size_t firstUselessSegIdx, 
            const file_system::DirectoryPtr& rootDirectory);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineRecoverStrategy);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_OFFLINE_RECOVER_STRATEGY_H
