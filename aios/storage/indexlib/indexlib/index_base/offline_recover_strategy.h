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
#ifndef __INDEXLIB_OFFLINE_RECOVER_STRATEGY_H
#define __INDEXLIB_OFFLINE_RECOVER_STRATEGY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, Version);

namespace indexlib { namespace index_base {

class OfflineRecoverStrategy
{
public:
    OfflineRecoverStrategy();
    ~OfflineRecoverStrategy();

public:
    static index_base::Version
    Recover(index_base::Version version, const file_system::DirectoryPtr& physicalDir,
            config::RecoverStrategyType recoverType = config::RecoverStrategyType::RST_SEGMENT_LEVEL);

    static void RemoveLostSegments(index_base::Version version, const file_system::DirectoryPtr& physicalDir);

    static void RemoveUselessSegments(index_base::Version version, const file_system::DirectoryPtr& physicalDir);

private:
    typedef std::pair<segmentid_t, std::string> SegmentInfoPair;

private:
    static void GetLostSegments(const file_system::DirectoryPtr& directory, const index_base::Version& version,
                                segmentid_t lastSegmentId, std::vector<SegmentInfoPair>& lostSegments);

    static void ReadSegmentTemperatureMeta(const index_base::SegmentInfo& segmentInfo, segmentid_t segmentId,
                                           Version& version);

    static void CleanUselessSegments(const std::vector<SegmentInfoPair>& lostSegments, size_t firstUselessSegIdx,
                                     const file_system::DirectoryPtr& rootDirectory);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineRecoverStrategy);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_OFFLINE_RECOVER_STRATEGY_H
