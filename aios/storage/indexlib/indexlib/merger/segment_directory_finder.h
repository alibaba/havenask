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

#include <string>

#include "indexlib/base/Types.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class SegmentDirectoryFinder
{
public:
    SegmentDirectoryFinder();
    ~SegmentDirectoryFinder();

public:
    static file_system::DirectoryPtr GetAttributeDir(const SegmentDirectoryPtr& segDir,
                                                     const config::AttributeConfig* attrConfig, segmentid_t segId);

    static segmentid_t GetSegmentIdFromDelMapDataFile(const SegmentDirectoryPtr& segDir, segmentid_t segmentId,
                                                      const std::string& dataFileName);

    static file_system::DirectoryPtr MakeSegmentDirectory(const file_system::DirectoryPtr& rootDirectory,
                                                          segmentid_t segId, const index_base::Version& version,
                                                          bool isSub = false);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDirectoryFinder);
}} // namespace indexlib::merger
