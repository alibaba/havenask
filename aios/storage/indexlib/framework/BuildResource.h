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

#include <limits>
#include <memory>

#include "indexlib/base/Types.h"
#include "kmonitor/client/MetricsReporter.h"

namespace autil {
class ThreadPool;
} // namespace autil

namespace indexlib::util {
class CounterMap;
class BuildResourceMetrics;
} // namespace indexlib::util

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2 {
class MemoryQuotaController;
namespace framework {
class MetricsManager;
class IIndexMemoryReclaimer;
class BuildDocumentMetrics;
} // namespace framework
} // namespace indexlibv2

namespace indexlibv2::framework {

struct BuildResource {
    std::shared_ptr<MemoryQuotaController> memController;
    std::shared_ptr<indexlib::util::CounterMap> counterMap;
    int64_t buildingMemLimit = std::numeric_limits<int64_t>::max();

    std::string counterPrefix;
    IIndexMemoryReclaimer* indexMemoryReclaimer = nullptr;
    MetricsManager* metricsManager = nullptr;
    std::shared_ptr<indexlib::util::BuildResourceMetrics> buildResourceMetrics;
    BuildDocumentMetrics* buildDocumentMetrics = nullptr;
    std::vector<std::pair<segmentid_t, std::shared_ptr<indexlib::file_system::Directory>>> segmentDirs;

    std::shared_ptr<autil::ThreadPool> consistentModeBuildThreadPool = nullptr;
    std::shared_ptr<autil::ThreadPool> inconsistentModeBuildThreadPool = nullptr;
};

} // namespace indexlibv2::framework
