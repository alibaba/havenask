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
#include "indexlib/index_base/lifecycle_table_construct.h"

#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(partition, LifecycleTableConstruct);

LifecycleTableConstruct::LifecycleTableConstruct() {}

LifecycleTableConstruct::~LifecycleTableConstruct() {}

bool LifecycleTableConstruct::ConstructLifeCycleTable(const std::string& rawIndexPath, versionid_t targetVersion,
                                                      file_system::LifecycleTablePtr& lifeCycle)
{
    Version version;
    try {
        VersionLoader::GetVersionS(rawIndexPath, version, targetVersion);
    } catch (...) {
        IE_LOG(ERROR, "get path [%s] target version [%d] failed", rawIndexPath.c_str(), targetVersion);
        return false;
    }
    const Version::SegTemperatureVec& metas = version.GetSegTemperatureMetas();
    for (const auto& meta : metas) {
        string segmentDir = version.GetSegmentDirName(meta.segmentId);
        string property = meta.segTemperature;
        lifeCycle->AddDirectory(segmentDir, property);
    }
    return true;
}

}} // namespace indexlib::index_base
