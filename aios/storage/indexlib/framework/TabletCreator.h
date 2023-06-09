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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/framework/ITablet.h"

namespace kmonitor {
class MetricsReporter;
}

namespace future_lite {
class Executor;
class TaskScheduler;
} // namespace future_lite

namespace indexlib::util {
class SearchCache;
}

namespace indexlib::file_system {
class FileBlockCacheContainer;
}

namespace indexlib::framework {
class TabletId;
}

namespace indexlibv2 {
class MemoryQuotaController;

namespace framework {

struct TabletResource;
class TabletCreator;
class ITabletMergeController;
class IdGenerator;

class TabletCreator final : private autil::NoCopyable
{
public:
    TabletCreator();
    ~TabletCreator();

public:
    TabletCreator& SetTabletId(const indexlib::framework::TabletId& tid);
    TabletCreator& SetExecutor(future_lite::Executor* dumpExecutor, future_lite::TaskScheduler* taskScheduler);
    TabletCreator& SetMemoryQuotaController(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                                            const std::shared_ptr<MemoryQuotaController>& buildMemoryQuotaController);
    TabletCreator& SetMetricsReporter(const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter);
    TabletCreator& SetFileBlockCacheContainer(
        const std::shared_ptr<indexlib::file_system::FileBlockCacheContainer>& fileBlockCacheContainer);
    TabletCreator& SetSearchCache(const std::shared_ptr<indexlib::util::SearchCache>& searchCache);
    TabletCreator& SetMergeController(const std::shared_ptr<ITabletMergeController>& mergeController);
    TabletCreator& SetIdGenerator(const std::shared_ptr<IdGenerator>& idGenerator);
    TabletCreator& SetRegisterTablet(bool registerTablet);

    std::shared_ptr<ITablet> CreateTablet();

private:
    std::unique_ptr<TabletResource> _tabletResource;
    bool _registerTablet = true;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace framework
} // namespace indexlibv2
