#ifndef __INDEXLIB_BUCKET_MAP_CREATOR_H
#define __INDEXLIB_BUCKET_MAP_CREATOR_H

#include <tr1/memory>
#include <autil/ThreadPool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_common.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/config/truncate_profile.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/util/resource_control_thread_pool.h"

IE_NAMESPACE_BEGIN(index);

class BucketMapCreator
{
public:
    BucketMapCreator();
    ~BucketMapCreator();
public:
    static BucketMaps CreateBucketMaps(
            const config::TruncateOptionConfigPtr& truncOptionConfig,
            TruncateAttributeReaderCreator *attrReaderCreator,
            int64_t maxMemUse = std::numeric_limits<int64_t>::max());
protected:
    static uint32_t GetBucketCount(uint32_t newDocCount);
    static bool NeedCreateBucketMap(const config::TruncateProfile& truncateProfile, 
                                    const config::TruncateStrategy& truncateStrategy,
                                    TruncateAttributeReaderCreator *attrReaderCreator);
    static util::ResourceControlThreadPoolPtr CreateBucketMapThreadPool(
            uint32_t truncateProfileSize,
            uint32_t totalDocCount, int64_t maxMemUse);

private:
    //for test
    friend class BucketMapCreatorTest;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BucketMapCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUCKET_MAP_CREATOR_H
