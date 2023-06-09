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

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/result/Result.h"
#include "navi/engine/Resource.h"
#include "ha3/sql/resource/watermark/TabletWaiterInitOption.h"

namespace indexlibv2::framework {
class ITablet;
} // namespace indexlibv2::framework

namespace indexlib::partition {
class IndexApplication;
} // namespace indexlib::partition

namespace isearch {
namespace sql {

class TabletWaiterBase;
class SqlBizResource;

class TabletManagerR : public navi::Resource {
private:
    typedef std::function<void(
        autil::result::Result<std::shared_ptr<indexlibv2::framework::ITablet>>)>
        CallbackFunc;

public:
    TabletManagerR();
    ~TabletManagerR();
    TabletManagerR(const TabletManagerR &) = delete;
    TabletManagerR& operator=(const TabletManagerR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public: // virtual for test
    virtual void waitTabletByWatermark(const std::string &tableName,
                                       int64_t watermark,
                                       CallbackFunc cb,
                                       int64_t timeoutUs);
    virtual void waitTabletByTargetTs(const std::string &tableName,
                                      int64_t targetTs,
                                      CallbackFunc cb,
                                      int64_t timeoutUs);
    virtual std::shared_ptr<indexlibv2::framework::ITablet>
    getTablet(const std::string &tableName) const;

private:
    bool canSupportWaterMark(const TabletWaiterInitOption& option);
private:
    SqlBizResource *_sqlBizResource = nullptr;
    std::map<std::string, std::unique_ptr<TabletWaiterBase>> _tabletWaiterMap;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TabletManagerR> TabletManagerRPtr;

} // namespace sql
} // namespace isearch
