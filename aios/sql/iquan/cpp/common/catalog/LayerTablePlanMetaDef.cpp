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
#include "iquan/common/catalog/LayerTablePlanMetaDef.h"

#include <unordered_map>

namespace iquan {

const static std::unordered_map<std::string, CompareOp> STRING2OP {
    {"GREATER_THAN", CompareOp::GREATER_THAN},
    {"GREATER_THAN_OR_EQUAL", CompareOp::GREATER_THAN_OR_EQUAL},
    {"LESS_THAN", CompareOp::LESS_THAN},
    {"LESS_THAN_OR_EQUAL", CompareOp::LESS_THAN_OR_EQUAL},
    {"EQUALS", CompareOp::EQUALS}};

LayerTablePlanMeta::LayerTablePlanMeta(const LayerTablePlanMetaDef &metaDef)
    : layerTableName(metaDef.layerTableName)
    , fieldName(metaDef.fieldName)
    , dynamicParamsIndex(metaDef.dynamicParamsIndex)
    , isRev(metaDef.isRev)
    , value(metaDef.value) {
    const std::string &strOp = metaDef.op;
    op = STRING2OP.at(strOp);
};

} // namespace iquan
