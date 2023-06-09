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
#include "indexlib/partition/unused_segment_collector.h"

#include "indexlib/index_base/index_meta/version_loader.h"

using namespace std;
using namespace fslib;

using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, UnusedSegmentCollector);

UnusedSegmentCollector::UnusedSegmentCollector() {}

UnusedSegmentCollector::~UnusedSegmentCollector() {}

void UnusedSegmentCollector::Collect(const ReaderContainerPtr& readerContainer, const DirectoryPtr& directory,
                                     FileList& segments)
{
    Version latestVersion;
    VersionLoader::GetVersion(directory, latestVersion, INVALID_VERSION);
    fslib::FileList tmpSegments;
    VersionLoader::ListSegments(directory, tmpSegments);
    for (size_t i = 0; i < tmpSegments.size(); ++i) {
        segmentid_t segId = Version::GetSegmentIdByDirName(tmpSegments[i]);
        if (segId > latestVersion.GetLastSegment()) {
            continue;
        }
        if (latestVersion.HasSegment(segId)) {
            continue;
        }
        if (readerContainer->IsUsingSegment(segId)) {
            continue;
        }
        segments.push_back(tmpSegments[i]);
    }
}
}} // namespace indexlib::partition
