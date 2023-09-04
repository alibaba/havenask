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

#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/sdk/IndexProvider.h"
#include "suez/sdk/SearchManager.h"

namespace suez {
class ConfigManager;
class CounterReporter;
struct InitParam;

class SearchManagerUpdater {
public:
    SearchManagerUpdater(bool allowPartialTableReady = false);
    ~SearchManagerUpdater();

public:
    AppMeta getAppMeta() const;
    // TODO: remove target
    BizMetas getBizMap(const HeartbeatTarget &target) const;
    SuezError getGlobalError() const;
    bool init(const InitParam &param);
    UPDATE_RESULT update(const HeartbeatTarget &target,
                         const SchedulerInfo &schedulerInfo,
                         const IndexProviderPtr &indexProvider,
                         UpdateIndications &updateIndications,
                         bool stopFlag = false);
    void cleanResource(const HeartbeatTarget &target);
    void stopService();
    void stopWorker();
    std::shared_ptr<IndexProvider> getIndexProviderSnapshot() const;
    ServiceInfo getServiceInfoSnapshot() const;
    autil::legacy::json::JsonMap getServiceInfo() const;
    TableUserDefinedMetas getTableMetas() const;
    SearchManager *getSearchManager() const;
    void reportMetrics();

private:
    struct SearchMetas {
    public:
        SearchMetas() { clear(); }
        void clear() {
            _globalError = ERROR_NONE;
            _bizMetasSnapshot.clear();
            _serviceInfoSnapshot = ServiceInfo();
            _appMetaSnapshot = AppMeta();
            _indexProviderSnapshot = std::make_unique<IndexProvider>();
            _customAppInfoSnapshot.clear();
            _tableMetas.clear();
        }

    public:
        SuezError _globalError;

        BizMetas _bizMetasSnapshot;
        AppMeta _appMetaSnapshot;
        ServiceInfo _serviceInfoSnapshot;
        std::shared_ptr<IndexProvider> _indexProviderSnapshot;
        autil::legacy::json::JsonMap _customAppInfoSnapshot;
        TableUserDefinedMetas _tableMetas;
    };
    using SearchMetasPtr = std::shared_ptr<SearchMetas>;

private:
    UPDATE_RESULT downloadConfig(const HeartbeatTarget &constTarget);
    void fillError(const SuezErrorCollector &errorCollector);
    UPDATE_RESULT doUpdate(const UpdateArgs &updateArgs,
                           UpdateIndications &updateIndications,
                           suez::SuezErrorCollector &errorCollector);
    bool needUpdate(const UpdateArgs &args) const;
    void updateSnapshot(const UpdateArgs &args);

private:
    bool _allowPartialTableReady;
    std::unique_ptr<ConfigManager> _configManager;
    std::unique_ptr<SearchManager> _searchManager;
    std::unique_ptr<SearchManager> _builtinService;
    std::unique_ptr<CounterReporter> _counterReporter;

    mutable autil::ThreadMutex _lock;
    SearchMetasPtr _searchMetas;
};

} // namespace suez
