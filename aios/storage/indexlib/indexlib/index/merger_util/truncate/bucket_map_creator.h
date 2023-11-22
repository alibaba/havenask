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

#include "autil/ThreadPool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/config/truncate_profile.h"
#include "indexlib/index/merger_util/truncate/bucket_map.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_common.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/resource_control_thread_pool.h"

namespace indexlib::index::legacy {

class BucketMapCreator
{
public:
    BucketMapCreator();
    ~BucketMapCreator();

public:
    static BucketMaps CreateBucketMaps(const config::TruncateOptionConfigPtr& truncOptionConfig,
                                       TruncateAttributeReaderCreator* attrReaderCreator,
                                       int64_t maxMemUse = std::numeric_limits<int64_t>::max());

protected:
    static uint32_t GetBucketCount(uint32_t newDocCount);
    static bool NeedCreateBucketMap(const config::TruncateProfile& truncateProfile,
                                    const config::TruncateStrategy& truncateStrategy,
                                    TruncateAttributeReaderCreator* attrReaderCreator);
    static util::ResourceControlThreadPoolPtr CreateBucketMapThreadPool(uint32_t truncateProfileSize,
                                                                        uint32_t totalDocCount, int64_t maxMemUse);

private:
    // for test
    friend class BucketMapCreatorTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BucketMapCreator);
} // namespace indexlib::index::legacy
