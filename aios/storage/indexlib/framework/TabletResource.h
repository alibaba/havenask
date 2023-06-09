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
#include <string>

#include "indexlib/framework/TabletId.h"

namespace future_lite {
class Executor;
class TaskScheduler;
} // namespace future_lite

namespace autil {
class ThreadPool;
} // namespace autil

namespace indexlibv2 {
class MemoryQuotaController;
}

namespace kmonitor {
class MetricsReporter;
}

namespace indexlib::util {
class SearchCache;
}

namespace indexlib::file_system {
class FileBlockCacheContainer;
}

namespace indexlibv2::framework {

class ITabletMergeController;
class IdGenerator;

struct TabletResource {
    // executors
    future_lite::Executor* dumpExecutor = nullptr;
    future_lite::TaskScheduler* taskScheduler = nullptr;

    std::shared_ptr<autil::ThreadPool> consistentModeBuildThreadPool = nullptr;
    std::shared_ptr<autil::ThreadPool> inconsistentModeBuildThreadPool = nullptr;

    // memory control
    std::shared_ptr<MemoryQuotaController> memoryQuotaController;
    std::shared_ptr<MemoryQuotaController> buildMemoryQuotaController;
    std::shared_ptr<indexlib::file_system::FileBlockCacheContainer> fileBlockCacheContainer;
    std::shared_ptr<indexlib::util::SearchCache> searchCache;
    std::shared_ptr<kmonitor::MetricsReporter> metricsReporter;

    // merge
    std::shared_ptr<ITabletMergeController> mergeController;
    // version id & segment id generate
    std::shared_ptr<IdGenerator> idGenerator;

    indexlib::framework::TabletId tabletId;
};

} // namespace indexlibv2::framework
