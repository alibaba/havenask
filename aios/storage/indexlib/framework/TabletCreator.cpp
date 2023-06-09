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
#include "indexlib/framework/TabletCreator.h"

#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/TabletCenter.h"
#include "indexlib/framework/TabletId.h"
#include "indexlib/framework/TabletResource.h"

using namespace std;

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletCreator);

TabletCreator::TabletCreator() : _tabletResource(new TabletResource) {}

TabletCreator::~TabletCreator() {}

TabletCreator& TabletCreator::SetExecutor(future_lite::Executor* dumpExecutor,
                                          future_lite::TaskScheduler* taskScheduler)
{
    _tabletResource->dumpExecutor = dumpExecutor;
    _tabletResource->taskScheduler = taskScheduler;
    return *this;
}

TabletCreator&
TabletCreator::SetMemoryQuotaController(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                                        const std::shared_ptr<MemoryQuotaController>& buildMemoryQuotaController)
{
    _tabletResource->memoryQuotaController = memoryQuotaController;
    _tabletResource->buildMemoryQuotaController = buildMemoryQuotaController;
    return *this;
}

TabletCreator& TabletCreator::SetMetricsReporter(const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter)
{
    _tabletResource->metricsReporter = metricsReporter;
    return *this;
}

TabletCreator& TabletCreator::SetFileBlockCacheContainer(
    const std::shared_ptr<indexlib::file_system::FileBlockCacheContainer>& fileBlockCacheContainer)
{
    _tabletResource->fileBlockCacheContainer = fileBlockCacheContainer;
    return *this;
}

TabletCreator& TabletCreator::SetSearchCache(const std::shared_ptr<indexlib::util::SearchCache>& searchCache)
{
    _tabletResource->searchCache = searchCache;
    return *this;
}

TabletCreator& TabletCreator::SetMergeController(const std::shared_ptr<ITabletMergeController>& mergeController)
{
    _tabletResource->mergeController = mergeController;
    return *this;
}

TabletCreator& TabletCreator::SetIdGenerator(const std::shared_ptr<IdGenerator>& idGenerator)
{
    _tabletResource->idGenerator = idGenerator;
    return *this;
}

TabletCreator& TabletCreator::SetTabletId(const indexlib::framework::TabletId& tid)
{
    _tabletResource->tabletId = tid;
    return *this;
}

TabletCreator& TabletCreator::SetRegisterTablet(bool registerTablet)
{
    _registerTablet = registerTablet;
    return *this;
}

std::shared_ptr<ITablet> TabletCreator::CreateTablet()
{
    auto tablet = std::make_shared<Tablet>(*_tabletResource);
    if (_registerTablet) {
        auto handler = TabletCenter::GetInstance()->RegisterTablet(tablet);
        if (handler) {
            handler->SetDocumentParser(tablet->CreateDocumentParser());
            tablet->SetRunAfterCloseFunction(
                [ptr = tablet.get()]() { TabletCenter::GetInstance()->UnregisterTablet(ptr); });
        }
    }
    return tablet;
}

} // namespace indexlibv2::framework
