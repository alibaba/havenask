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
    void fillZoneUdfMap(const iquan::TableModels &tableModels);
    bool fillFunctionModels(iquan::FunctionModels &functionModels,
                            iquan::TvfModels &tvfModels) const;

private:
    template <typename IquanModels>
    static void addUserFunctionModels(const IquanModels &inModels,
                                      const std::vector<std::string> &specialCatalogs,
                                      const std::string &dbName,
                                      IquanModels &outModels);

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON_FALSE(AggFuncFactoryR, _aggFuncFactoryR);
    RESOURCE_DEPEND_ON_FALSE(TvfFuncFactoryR, _tvfFuncFactoryR);
    iquan::FunctionModels _defaultUdfFunctionModels;
    std::map<std::string, std::vector<iquan::FunctionModel>> _zoneUdfMap;
    std::vector<std::string> _specialCatalogs;
    std::map<std::string, iquan::FunctionModels> _zoneFunctionModels;
};

template <typename IquanModels>
void UdfModelR::addUserFunctionModels(const IquanModels &inModels,
                                      const std::vector<std::string> &specialCatalogs,
                                      const std::string &dbName,

                                      IquanModels &outModels) {

    for (auto model : inModels.functions) {
        if (!model.catalogName.empty()) {
            model.databaseName = dbName;
            model.functionVersion = 1;
            outModels.functions.emplace_back(model);
        } else {
            model.catalogName = SQL_DEFAULT_CATALOG_NAME;
            model.databaseName = dbName;
            model.functionVersion = 1;
            outModels.functions.emplace_back(model);
            for (auto const &catalogName : specialCatalogs) {
                model.catalogName = catalogName;
                model.databaseName = dbName;
                model.functionVersion = 1;
                outModels.functions.emplace_back(model);
            }
        }
    }
}

NAVI_TYPEDEF_PTR(UdfModelR);

} // namespace sql
