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
#include "indexlib/framework/lifecycle/LifecycleTableCreator.h"

#include "indexlib/file_system/LifecycleTable.h"
#include "indexlib/framework/lifecycle/LifecycleStrategyFactory.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, LifecycleTableCreator);

std::shared_ptr<indexlib::file_system::LifecycleTable>
LifecycleTableCreator::CreateLifecycleTable(const indexlibv2::framework::Version& version,
                                            const indexlib::file_system::LifecycleConfig& lifecycleConfig,
                                            const std::map<std::string, std::string>& parameters)
{
    auto lifecycleStrategy = indexlib::framework::LifecycleStrategyFactory::CreateStrategy(lifecycleConfig, parameters);
    if (lifecycleStrategy == nullptr) {
        AUTIL_LOG(ERROR, "Create LifecycleStrategy failed for version[%d]", version.GetVersionId());
        return nullptr;
    }
    auto segLifecycles = lifecycleStrategy->GetSegmentLifecycles(version.GetSegmentDescriptions());
    auto lifecycleTable = std::make_shared<indexlib::file_system::LifecycleTable>();
    std::set<std::string> segmentsWithLifecycle;
    for (const auto& kv : segLifecycles) {
        std::string segmentDir = version.GetSegmentDirName(kv.first);
        lifecycleTable->AddDirectory(segmentDir, kv.second);
        segmentsWithLifecycle.insert(segmentDir);
    }
    for (auto it = version.begin(); it != version.end(); it++) {
        std::string segmentDir = version.GetSegmentDirName(it->segmentId);
        if (segmentsWithLifecycle.find(segmentDir) == segmentsWithLifecycle.end()) {
            lifecycleTable->AddDirectory(segmentDir, "");
        }
    }
    return lifecycleTable;
}

} // namespace indexlibv2::framework
