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

#include <string>

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/rpc_server/NaviRpcServerR.h"

namespace navi {
class GraphDef;
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class SqlRpcR : public navi::Resource {
public:
    SqlRpcR();
    ~SqlRpcR();
    SqlRpcR(const SqlRpcR &) = delete;
    SqlRpcR &operator=(const SqlRpcR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

private:
    bool registerSql();
    bool registerClientInfo();
    bool buildRunSqlGraph(navi::GraphDef *graphDef) const;

public:
    static const std::string RESOURCE_ID;
    static const std::string SQL_REQUEST_NAME;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(navi::NaviRpcServerR, _naviRpcServerR);
};

NAVI_TYPEDEF_PTR(SqlRpcR);

} // namespace sql
