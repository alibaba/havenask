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

#include <atomic>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/legacy/json.h"
#include "future_lite/CoroInterface.h"
#include "sap/common/common.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/IndexProvider.h"
#include "suez/sdk/SchedulerInfo.h"
#include "suez/sdk/SearchInitParam.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/sdk/SuezError.h"
#include "suez/sdk/TableUserDefinedMeta.h"

namespace sap {
class ServerWorkerContext;
class ServerWorkerHandler;
class Updater;
} // namespace sap

namespace iquan {
class CatalogInfo;
}

namespace suez {

struct SearchInitParam;

struct UpdateArgs {
    UpdateArgs() = default;

    UpdateArgs(const IndexProviderPtr &indexProvider_,
               const BizMetas &bizMetas_,
               const AppMeta &appMeta_,
               const ServiceInfo &serviceInfo_,
               const SchedulerInfo &schedulerInfo_,
               const autil::legacy::json::JsonMap &customAppInfo_,
               const TableUserDefinedMetas &tableMetas_,
               bool stopFlag_)
        : indexProvider(indexProvider_)
        , bizMetas(bizMetas_)
        , appMeta(appMeta_)
        , serviceInfo(serviceInfo_)
        , schedulerInfo(schedulerInfo_)
        , customAppInfo(customAppInfo_)
        , tableMetas(tableMetas_)
        , stopFlag(stopFlag_) {}

    IndexProviderPtr indexProvider;
    BizMetas bizMetas;
    AppMeta appMeta;
    ServiceInfo serviceInfo;
    SchedulerInfo schedulerInfo;
    autil::legacy::json::JsonMap customAppInfo;
    TableUserDefinedMetas tableMetas;
    bool stopFlag;
};

struct UpdateIndications {
    std::set<PartitionId> releaseLeaderTables;
    std::set<PartitionId> campaginLeaderTables;
    std::set<PartitionId> publishLeaderTables;
    std::map<PartitionId, int32_t> tablesWeight;
};

class SearchManager {
public:
    SearchManager();
    virtual ~SearchManager();

private:
    SearchManager(const SearchManager &);
    SearchManager &operator=(const SearchManager &);

public:
    virtual bool init(const SearchInitParam &initParam);
    virtual bool needForceUpdate(const UpdateArgs &args) { return false; }
    virtual UPDATE_RESULT update(const suez::UpdateArgs &updateArgs,
                                 UpdateIndications &updateIndications,
                                 suez::SuezErrorCollector &errorCollector) = 0;

    //@recommended
    virtual void stopService() = 0;
    virtual void stopWorker() = 0;

    // you can return service-discovery topoinfo from here
    virtual autil::legacy::json::JsonMap getServiceInfo() const;

    // TODO: remove this after migrate sql to suez_navi
    virtual std::unique_ptr<iquan::CatalogInfo> getCatalogInfo() const;

    virtual sap::ret_t sapSearch(sap::ServerWorkerContext &context) { return sap::r_failed; }
    virtual FL_LAZY(sap::ret_t) sapSearchAsync(sap::ServerWorkerContext &context) { FL_CORETURN sap::r_failed; }
    virtual bool serviceAvailable(const char *path) { return false; }
    virtual sap::ServerWorkerHandler *makeWorkerHandler() { return NULL; }
    virtual sap::Updater *makeUpdater() { return NULL; }

    void updateStatus(bool status);
    void updateIssuedVersionTime(int64_t t);
    void updateTableVersionTime(int64_t t);
    void updateVersionTime(int64_t t);

protected:
    std::atomic<bool> _innerUpdatingStatus = {false};
    std::atomic<int64_t> _lastIssuedVersionTimestamp = {0};
    std::atomic<int64_t> _lastUpdatedTableVersionTimestamp = {0};
    std::atomic<int64_t> _lastUpdatedVersionTimestamp = {0};
};

SearchManager *createSearchManager() __attribute__((weak));

} // namespace suez
