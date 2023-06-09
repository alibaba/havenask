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
#include "ha3/sql/ops/tvf/TvfFuncManager.h"
#include "ha3/sql/ops/tvf/SqlTvfPluginConfig.h"

namespace suez_navi {

class TvfFuncManagerR : public navi::Resource
{
public:
    TvfFuncManagerR();
    ~TvfFuncManagerR();
    TvfFuncManagerR(const TvfFuncManagerR &) = delete;
    TvfFuncManagerR &operator=(const TvfFuncManagerR &) = delete;
public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
public:
    const isearch::sql::TvfFuncManagerPtr &getManager() const;
public:
    static const std::string RESOURCE_ID;
private:
    std::string _configPath;
    isearch::sql::SqlTvfPluginConfig _tvfConfig;
    isearch::sql::TvfFuncManagerPtr _manager;
};

NAVI_TYPEDEF_PTR(TvfFuncManagerR);

}

