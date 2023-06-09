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
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/summary/SummaryProfileManager.h"
#include "suez/turing/common/QueryResource.h"
#include "ha3/turing/common/SqlBiz.h"

namespace isearch {
namespace proto {
class Range;
} // namespace proto
namespace sql {
class TvfSummaryResource;
} // namespace sql
namespace turing {
class Ha3BizMeta;
class Ha3ServiceSnapshot;
class Ha3ClusterDef;
} // namespace turing
} // namespace isearch

namespace isearch {
namespace sql {

class DefaultSqlBiz : public isearch::turing::SqlBiz {
public:
    DefaultSqlBiz(isearch::turing::Ha3ServiceSnapshot *snapshot,
                  isearch::turing::Ha3BizMeta *ha3BizMeta);
    ~DefaultSqlBiz();

private:
    DefaultSqlBiz(const DefaultSqlBiz &);
    DefaultSqlBiz &operator=(const DefaultSqlBiz &);

public:
    bool updateNavi();

protected:
    suez::turing::QueryResourcePtr createQueryResource() override;
    bool getDefaultBizJson(std::string &defaultBizJson) override;
    std::string getBizInfoFile() override;
    tensorflow::Status loadUserPlugins() override;
private:
    tensorflow::Status initUserMetadata() override;
    std::string convertFunctionConf();
    static bool getRange(uint32_t partCount, uint32_t partId, proto::Range &range);
    isearch::sql::TvfSummaryResource *perpareTvfSummaryResource();
    bool createSummaryConfigMgr(isearch::summary::SummaryProfileManagerPtr &summaryProfileMgrPtr,
                                isearch::config::HitSummarySchemaPtr &hitSummarySchemaPtr,
                                const std::string &clusterName,
                                const suez::turing::TableInfoPtr &tableInfoPtr);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DefaultSqlBiz> DefaultSqlBizPtr;
} // namespace sql
} // namespace isearch
