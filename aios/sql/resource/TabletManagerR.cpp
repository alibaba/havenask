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
#include "sql/resource/TabletManagerR.h"

#include <engine/ResourceInitContext.h>
#include <iosfwd>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/result/Errors.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/partition/index_application.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/resource/watermark/TabletWaiter.h"
#include "sql/resource/watermark/TabletWaiterBase.h"
#include "sql/resource/watermark/TabletWaiterInitOption.h"
#include "suez/sdk/IndexProvider.h"
#include "suez/sdk/PartitionId.h"
#include "suez/sdk/SourceReaderProvider.h"

using namespace std;

namespace sql {

const std::string TabletManagerR::RESOURCE_ID = "sql.tablet_manager_r";

TabletManagerR::TabletManagerR() {}

TabletManagerR::~TabletManagerR() {}

void TabletManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ_PART);
}

bool TabletManagerR::config(navi::ResourceConfigContext &ctx) {
    return true;
}
bool TabletManagerR::canSupportWaterMark(const TabletWaiterInitOption &option) {
    if (!option.realtimeInfo.isValid()) {
        return false;
    }
    const auto &kvMap = option.realtimeInfo.getKvMap();
    auto iter = kvMap.find(build_service::config::REALTIME_MODE);
    if (iter == kvMap.end()) {
        return false;
    }
    if (iter->second == build_service::config::REALTIME_JOB_MODE
        || iter->second == build_service::config::REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE) {
        return true;
    }
    return false;
}

navi::ErrorCode TabletManagerR::init(navi::ResourceInitContext &ctx) {
    auto partId = ctx.getPartId();
    const auto &indexAppMap = _tableInfoR->getIndexAppMap();
    const auto &indexProvider = _indexProviderR->getIndexProvider();
    const auto &sourceProvider = indexProvider.sourceReaderProvider;
    auto it = indexAppMap.find(partId);
    if (it == indexAppMap.end()) {
        SQL_LOG(INFO, "index app not found for partId[%d], skip", partId);
        return navi::EC_NONE;
    }

    auto tablets = it->second->GetAllTablets();
    SQL_LOG(INFO, "init from index app, tablets[%s]", autil::StringUtil::toString(tablets).c_str());
    for (const auto &tablet : tablets) {
        std::string tableName = tablet->GetTabletSchema()->GetTableName();
        auto *sourceConfig = sourceProvider.getSourceConfigByIdx(tableName, partId);
        if (!sourceConfig) {
            SQL_LOG(ERROR, "get swift config for table name[%s] failed", tableName.c_str());
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
            SQL_LOG(ERROR, "tablet waiter init failed, table name[%s]", tableName.c_str());
            return navi::EC_ABORT;
        }
        auto res = _tabletWaiterMap.emplace(tableName, std::move(waiter));
        if (!res.second) {
            SQL_LOG(ERROR, "table name[%s] conflict", tableName.c_str());
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
