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
#include "suez/search/SearchManagerUpdater.h"

#include <assert.h>
#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/EnvUtil.h"
#include "autil/Lock.h"
#include "autil/SharedPtrUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "suez/common/InitParam.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/JsonNodeRef.h"
#include "suez/sdk/SearchInitParam.h"
#include "suez/search/ConfigManager.h"
#include "suez/search/CounterReporter.h"
#include "suez/service/BuiltinSearchManagerList.h"
#include "suez/service/ControlServiceImpl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SearchManagerUpdater);

SearchManagerUpdater::SearchManagerUpdater(bool allowPartialTableReady)
    : _allowPartialTableReady(allowPartialTableReady)
    , _configManager(new ConfigManager())
    , _counterReporter(new CounterReporter())
    , _searchMetas(new SearchMetas()) {
    _builtinService = std::make_unique<BuiltinSearchManagerList>();
}

SearchManagerUpdater::~SearchManagerUpdater() {}

AppMeta SearchManagerUpdater::getAppMeta() const { return _searchMetas->_appMetaSnapshot; }

BizMetas SearchManagerUpdater::getBizMap(const HeartbeatTarget &target) const {
    BizMetas retMetas;
    for (const auto &biz : _searchMetas->_bizMetasSnapshot) {
        // fill configPath
        auto &meta = retMetas[biz.first];
        meta.setRemoteConfigPath(biz.second.getRemoteConfigPath());
        meta.setKeepCount(biz.second.getKeepCount());
    }
    BizMetas bizMetas;
    AppMeta appMeta;
    _configManager->getTargetMetas(target, bizMetas, appMeta);

    for (const auto &biz : bizMetas) {
        // fill error
        auto &meta = retMetas[biz.first];
        meta.setError(biz.second.getError());
        meta.setKeepCount(biz.second.getKeepCount());
    }
    return retMetas;
}

SuezError SearchManagerUpdater::getGlobalError() const { return _searchMetas->_globalError; }

bool SearchManagerUpdater::init(const InitParam &initParam) {
    if (!_configManager->init(initParam)) {
        return false;
    }

    _counterReporter->init(initParam.kmonMetaInfo);

    if (createSearchManager) {
        _searchManager.reset(createSearchManager());
        if (!_searchManager) {
            AUTIL_LOG(ERROR, "createSearchManager failed");
            return false;
        }
    } else {
        AUTIL_LOG(INFO, "suez standalone mode, ControlService embedded");
        _searchManager.reset(new ControlServiceImpl());
    }

    SearchInitParam searchInitParam;
    searchInitParam.rpcServer = initParam.rpcServer;
    searchInitParam.kmonMetaInfo = initParam.kmonMetaInfo;
    searchInitParam.installRoot = initParam.installRoot;
    searchInitParam.asyncInterExecutor = initParam.asyncInterExecutor;
    searchInitParam.asyncIntraExecutor = initParam.asyncIntraExecutor;

    if (!_builtinService->init(searchInitParam)) {
        AUTIL_LOG(ERROR, "init builtin service failed");
        return false;
    }

    return _searchManager->init(searchInitParam);
}

UPDATE_RESULT SearchManagerUpdater::update(const HeartbeatTarget &target,
                                           const SchedulerInfo &schedulerInfo,
                                           const IndexProviderPtr &indexProvider,
                                           UpdateIndications &updateIndications,
                                           bool stopFlag) {
    if (!indexProvider) {
        return UR_NEED_RETRY;
    }

    UpdateArgs updateArgs;
    updateArgs.indexProvider = indexProvider;
    updateArgs.serviceInfo = target.getServiceInfo();
    updateArgs.schedulerInfo = schedulerInfo;
    updateArgs.customAppInfo = target.getCustomAppInfo();
    updateArgs.stopFlag = stopFlag;

    suez::SuezErrorCollector errorCollector;
    // 直写场景下, _builtinService无条件更新updateArgs
    [[maybe_unused]] UPDATE_RESULT ret = _builtinService->update(updateArgs, updateIndications, errorCollector);
    assert(ret == UR_REACH_TARGET);

    if (!_allowPartialTableReady && !indexProvider->allTableReady) {
        std::vector<std::string> notReadyTables;
        for (const auto &it : target.getTableMetas()) {
            const auto &pid = it.first;
            if (!indexProvider->multiTableReader.hasTableReader(pid)) {
                notReadyTables.push_back(FastToJsonString(pid, /*isCompact=*/true));
            }
        }
        AUTIL_LOG(INFO,
                  "not all table ready, do not update biz. waiting for readers [%s]",
                  StringUtil::toString(notReadyTables, "; ").c_str());
        return UR_NEED_RETRY;
    }
    int64_t newVersionTimestamp = target.getCompatibleTargetVersionTimestamp();
    assert(_searchManager);
    _searchManager->updateIssuedVersionTime(newVersionTimestamp);

    _searchManager->updateStatus(true);
    ret = downloadConfig(target);
    if (ret != UR_REACH_TARGET) {
        return ret;
    }

    _searchManager->updateTableVersionTime(newVersionTimestamp);

    BizMetas bizMetas;
    AppMeta appMeta;
    _configManager->getTargetMetas(target, bizMetas, appMeta);

    updateArgs.bizMetas = std::move(bizMetas);
    updateArgs.appMeta = appMeta;
    target.getTableMetas().GetTableUserDefinedMetas(updateArgs.tableMetas);
    ret = doUpdate(updateArgs, updateIndications, errorCollector);
    if (ret != UR_REACH_TARGET) {
        fillError(errorCollector);
        return ret;
    }
    _searchManager->updateStatus(false);
    _searchManager->updateVersionTime(newVersionTimestamp);

    return UR_REACH_TARGET;
}

UPDATE_RESULT SearchManagerUpdater::doUpdate(const UpdateArgs &updateArgs,
                                             UpdateIndications &updateIndications,
                                             suez::SuezErrorCollector &errorCollector) {
    if (!needUpdate(updateArgs)) {
        return UR_REACH_TARGET;
    }
    UPDATE_RESULT ret = _searchManager->update(updateArgs, updateIndications, errorCollector);
    if (ret == UR_REACH_TARGET) {
        updateSnapshot(updateArgs);
    }
    return ret;
}

void SearchManagerUpdater::cleanResource(const HeartbeatTarget &target) { _configManager->cleanResource(target); }

void SearchManagerUpdater::stopService() {
    if (_searchManager) {
        _searchManager->stopService();
    }
    _builtinService->stopService();
    ScopedLock lock(_lock);
    _searchMetas->clear();
}

void SearchManagerUpdater::stopWorker() {
    if (_searchManager) {
        _searchManager->stopWorker();
    }
    _builtinService->stopWorker();
    ScopedLock lock(_lock);
    _searchMetas->clear();
}

shared_ptr<IndexProvider> SearchManagerUpdater::getIndexProviderSnapshot() const {
    ScopedLock lock(_lock);
    return _searchMetas->_indexProviderSnapshot;
}

ServiceInfo SearchManagerUpdater::getServiceInfoSnapshot() const {
    ScopedLock lock(_lock);
    return _searchMetas->_serviceInfoSnapshot;
}

TableUserDefinedMetas SearchManagerUpdater::getTableMetas() const {
    ScopedLock lock(_lock);
    return _searchMetas->_tableMetas;
}

autil::legacy::json::JsonMap SearchManagerUpdater::getServiceInfo() const {
    if (!_searchManager) {
        return JsonMap();
    }
    return _searchManager->getServiceInfo();
}

SearchManager *SearchManagerUpdater::getSearchManager() const { return _searchManager.get(); }

UPDATE_RESULT SearchManagerUpdater::downloadConfig(const HeartbeatTarget &target) {
    auto ret = _configManager->update(target);
    _searchMetas->_globalError = _configManager->getGlobalError();
    return ret;
}

void SearchManagerUpdater::fillError(const SuezErrorCollector &errorCollector) {
    _configManager->fillError(errorCollector);
    _searchMetas->_globalError = errorCollector.getGlobalError();
}

bool SearchManagerUpdater::needUpdate(const UpdateArgs &args) const {
    return (*(_searchMetas->_indexProviderSnapshot) != *(args.indexProvider)) ||
           args.bizMetas != _searchMetas->_bizMetasSnapshot || args.serviceInfo != _searchMetas->_serviceInfoSnapshot ||
           args.appMeta != _searchMetas->_appMetaSnapshot ||
           FastToJsonString(args.customAppInfo) != FastToJsonString(_searchMetas->_customAppInfoSnapshot) ||
           _searchManager->needForceUpdate(args);
}

void SearchManagerUpdater::updateSnapshot(const UpdateArgs &args) {
    shared_ptr<IndexProvider> oldIndexProvider;
    {
        ScopedLock lock(_lock);
        oldIndexProvider = _searchMetas->_indexProviderSnapshot;
        _searchMetas->_bizMetasSnapshot = args.bizMetas;
        _searchMetas->_appMetaSnapshot = args.appMeta;
        _searchMetas->_serviceInfoSnapshot = args.serviceInfo;
        _searchMetas->_indexProviderSnapshot = args.indexProvider;
        _searchMetas->_customAppInfoSnapshot = JsonNodeRef::copy(args.customAppInfo);
        _searchMetas->_tableMetas.clear();
        for (auto it = args.tableMetas.begin(); it != args.tableMetas.end(); ++it) {
            _searchMetas->_tableMetas[it->first] = JsonNodeRef::copy(it->second);
        }
    }
    while (true) {
        if (autil::SharedPtrUtil::waitUseCount(oldIndexProvider, 1, 10 * 1000 * 1000l)) {
            break;
        }
        AUTIL_LOG(INFO,
                  "waiting use count for index snapshot, current[%lu] expect[1] ptr[%p]",
                  oldIndexProvider.use_count(),
                  oldIndexProvider.get());
    }
}

void SearchManagerUpdater::reportMetrics() {
    auto snapshot = getIndexProviderSnapshot();
    if (snapshot) {
        _counterReporter->report(*snapshot);
    }
}

} // namespace suez
