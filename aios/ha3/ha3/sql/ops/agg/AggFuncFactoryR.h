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
#include <set>
#include <stddef.h>
#include <string>
#include <vector>

#include "ha3/sql/ops/agg/AggFuncMode.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "table/DataCommon.h"

namespace isearch {
namespace sql {
class AggFunc;
class AggFuncCreatorR;
} // namespace sql
} // namespace isearch
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace isearch {
namespace sql {

class AggFuncFactoryR : public navi::Resource {
public:
    AggFuncFactoryR();
    ~AggFuncFactoryR();
    AggFuncFactoryR(const AggFuncFactoryR &) = delete;
    AggFuncFactoryR &operator=(const AggFuncFactoryR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    const iquan::FunctionModels &getFunctionModels() const;
    AggFunc *createAggFunction(const std::string &aggFuncName,
                               const std::vector<table::ValueType> &inputTypes,
                               const std::vector<std::string> &inputFields,
                               const std::vector<std::string> &outputFields,
                               AggFuncMode mode) const;

private:
    std::set<AggFuncCreatorR *> _aggFuncCreators;
    std::map<std::string, AggFuncCreatorR *> _funcToCreator;
    std::map<std::string, size_t> _accSize;
    iquan::FunctionModels _functionModels;

public:
    static const std::string RESOURCE_ID;
};
NAVI_TYPEDEF_PTR(AggFuncFactoryR);

} // namespace sql
} // namespace isearch
