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
#include "ha3/sql/resource/TabletManagerR.h"

#include "autil/result/Errors.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/watermark/TabletWaiter.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/partition/index_application.h"
#include "suez/sdk/IndexProvider.h"
#include "suez/sdk/SourceReaderProvider.h"

using namespace std;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, TabletManagerR);

TabletManagerR::TabletManagerR() {
}

TabletManagerR::~TabletManagerR() {}

void TabletManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(TABLET_MANAGER_RESOURCE_ID)
        .depend(SqlBizResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_sqlBizResource));
}

bool TabletManagerR::config(navi::ResourceConfigContext &ctx) {
    return true;
}
bool TabletManagerR::canSupportWaterMark(const TabletWaiterInitOption& option)
{
    if (!option.realtimeInfo.isValid()) {
        return false;
    }
    const auto& kvMap = option.realtimeInfo.getKvMap();
    auto iter = kvMap.find(build_service::config::REALTIME_MODE);
    if (iter == kvMap.end()) {
        return false;
    }
    if (iter->second == build_service::config::REALTIME_JOB_MODE ||
        iter->second == build_service::config::REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE)
    {
        return true;
    }
    return false;
}

navi::ErrorCode TabletManagerR::init(navi::ResourceInitContext &ctx) {
    auto partId = ctx.getPartId();
    auto *indexAppMap = _sqlBizResource->getIndexAppMap();
    auto *indexProvider = _sqlBizResource->getIndexProvider();
    auto &sourceProvider = indexProvider->sourceReaderProvider;
    auto it = indexAppMap->find(partId);
    if (it == indexAppMap->end()) {
        NAVI_KERNEL_LOG(INFO, "index app not found for partId[%d], skip", partId);
        return navi::EC_NONE;
    }

    auto tablets = it->second->GetAllTablets();
    NAVI_KERNEL_LOG(
        INFO, "init from index app, tablets[%s]", autil::StringUtil::toString(tablets).c_str());
    for (const auto &tablet : tablets) {
        std::string tableName = tablet->GetTabletSchema()->GetTableName();
        auto *sourceConfig = sourceProvider.getSourceConfigByIdx(tableName, partId);
        if (!sourceConfig) {
            NAVI_KERNEL_LOG(ERROR, "get swift config for table name[%s] failed", tableName.c_str());
            return navi::EC_ABORT;
        }
        TabletWaiterInitOption option;
        option.tablet = tablet;
        option.from = sourceConfig->pid.from;
        option.to = sourceConfig->pid.to;
        option.swiftClientCreator = sourceProvider.swiftClientCreator;
        option.realtimeInfo = sourceConfig->realtimeInfo;

        std::unique_ptr<TabletWaiterBase> waiter;
        if (canSupportWaterMark(option)) {
            SQL_LOG(INFO,
                    "realtime info set, enable waiter function, table name[%s]",
                    tableName.c_str());
            waiter.reset(new TabletWaiter(tablet));
        } else {
            SQL_LOG(INFO,
                    "realtime info not set, disable waiter function, table name[%s]",
                    tableName.c_str());
            waiter.reset(new TabletWaiterNoWatermark(tablet));
        }
        if (!waiter->init(option)) {
            NAVI_KERNEL_LOG(ERROR, "tablet waiter init failed, table name[%s]", tableName.c_str());
            return navi::EC_ABORT;
        }
        auto res = _tabletWaiterMap.emplace(tableName, std::move(waiter));
        if (!res.second) {
            NAVI_KERNEL_LOG(ERROR, "table name[%s] conflict", tableName.c_str());
            return navi::EC_ABORT;
        }
    }
    return navi::EC_NONE;
}

void TabletManagerR::waitTabletByWatermark(const std::string &tableName,
                                           int64_t watermark,
                                           CallbackFunc cb,
                                           int64_t timeoutUs) {
    auto it = _tabletWaiterMap.find(tableName);
    if (it != _tabletWaiterMap.end()) {
        it->second->waitTabletByWatermark(watermark, std::move(cb), timeoutUs);
    } else {
        cb(autil::result::RuntimeError::make("table name[%s] not found", tableName.c_str()));
    }
}

void TabletManagerR::waitTabletByTargetTs(const std::string &tableName,
                                          int64_t targetTs,
                                          CallbackFunc cb,
                                          int64_t timeoutUs) {
    auto it = _tabletWaiterMap.find(tableName);
    if (it != _tabletWaiterMap.end()) {
        it->second->waitTabletByTargetTs(targetTs, std::move(cb), timeoutUs);
    } else {
        cb(autil::result::RuntimeError::make("table name[%s] not found", tableName.c_str()));
    }
}

std::shared_ptr<indexlibv2::framework::ITablet>
TabletManagerR::getTablet(const std::string &tableName) const {
    auto it = _tabletWaiterMap.find(tableName);
    if (it != _tabletWaiterMap.end()) {
        return it->second->getTablet();
    } else {
        return nullptr;
    }
}

REGISTER_RESOURCE(TabletManagerR);

} // namespace sql
} // namespace isearch
