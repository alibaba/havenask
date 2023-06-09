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

#include "ha3/sql/common/TableDistribution.h"
#include "navi/engine/KernelConfigContext.h"

namespace isearch {
namespace sql {

struct TableModifyInitParam {
public:
    bool initFromJson(navi::KernelConfigContext &ctx);
private:
    void stripName();
public:
    std::string catalogName;
    std::string dbName;
    std::string tableName;
    std::string tableType;
    std::string pkField;
    std::string operation;
    std::string conditionJson;
    std::string modifyFieldExprsJson;
    std::vector<std::string> outputFields;
    std::vector<std::string> outputFieldsType;
    TableDistribution tableDist;
};

typedef std::shared_ptr<TableModifyInitParam> TableModifyInitParamPtr;

} // end namespace sql
} // end namespace isearch
