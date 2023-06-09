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

#include "navi/engine/Resource.h"
#include "ha3/sql/ops/agg/AggFuncManager.h"
#include "ha3/sql/ops/agg/SqlAggPluginConfig.h"

namespace suez_navi {

class AggFuncManagerR : public navi::Resource
{
public:
    AggFuncManagerR();
    ~AggFuncManagerR();
    AggFuncManagerR(const AggFuncManagerR &) = delete;
    AggFuncManagerR &operator=(const AggFuncManagerR &) = delete;
public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
public:
    const isearch::sql::AggFuncManagerPtr &getManager() const;
public:
    static const std::string RESOURCE_ID;
private:
    std::string _configPath;
    isearch::sql::SqlAggPluginConfig _aggConfig;
    isearch::sql::AggFuncManagerPtr _manager;
};

NAVI_TYPEDEF_PTR(AggFuncManagerR);

}

