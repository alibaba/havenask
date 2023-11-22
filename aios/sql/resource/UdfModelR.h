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

#include <map>
#include <string>
#include <vector>

#include "iquan/common/Common.h"
#include "iquan/common/catalog/CatalogDef.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/ops/agg/AggFuncFactoryR.h"
#include "sql/ops/tvf/TvfFuncFactoryR.h"

namespace iquan {
class TableModels;
class TvfModels;
} // namespace iquan
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class UdfModelR : public navi::Resource {
public:
    UdfModelR();
    ~UdfModelR();
    UdfModelR(const UdfModelR &) = delete;
    UdfModelR &operator=(const UdfModelR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    bool fillFunctionModels(iquan::CatalogDefs &catalogDefs);

private:
    static void mergeUdfs(std::vector<iquan::FunctionModel> &destUdfModels,
                          std::vector<iquan::FunctionModel> &srcUdfModels);
    static bool addFunctions(iquan::CatalogDefs &catalogDefs,
                             const std::vector<std::string> &catalogList,
                             const std::vector<iquan::FunctionModel> &functionModes,
                             const std::string &dbName);

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON_FALSE(AggFuncFactoryR, _aggFuncFactoryR);
    RESOURCE_DEPEND_ON_FALSE(TvfFuncFactoryR, _tvfFuncFactoryR);
    std::vector<iquan::FunctionModel> _defaultUdfModels;
    std::vector<iquan::FunctionModel> _systemUdfModels;
    std::map<std::string, std::vector<iquan::FunctionModel>> _dbUdfMap;
    std::vector<std::string> _specialCatalogs;
};

NAVI_TYPEDEF_PTR(UdfModelR);

} // namespace sql
