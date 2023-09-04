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
#include "sql/ops/scan/UseSubR.h"

#include <memory>
#include <vector>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/navi/TableInfoR.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string UseSubR::RESOURCE_ID = "sql.use_sub_r";

UseSubR::UseSubR() {}

UseSubR::~UseSubR() {}

void UseSubR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool UseSubR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode UseSubR::init(navi::ResourceInitContext &ctx) {
    if (!initUseSub()) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool UseSubR::initUseSub() {
    if (!_scanInitParamR->useNest) {
        _useSub = false;
        return true;
    }
    const auto &tableName = _scanInitParamR->tableName;
    auto tableInfo = _scanInitParamR->tableInfoR->getTableInfo(tableName);
    if (!tableInfo) {
        SQL_LOG(ERROR, "table [%s] not exist.", tableName.c_str());
        return false;
    }
    const auto &nestTableAttrs = _scanInitParamR->nestTableAttrs;
    const auto &subTableName = tableInfo->getSubTableName();
    for (const auto &nestTableAttr : nestTableAttrs) {
        if (subTableName == nestTableAttr.tableName) {
            _useSub = true;
            SQL_LOG(TRACE1, "table [%s] use sub doc.", tableName.c_str());
        } else {
            SQL_LOG(ERROR,
                    "table [%s] need unnest multi value, not support.[%s != %s]",
                    tableName.c_str(),
                    subTableName.c_str(),
                    nestTableAttr.tableName.c_str());
            return false;
        }
    }
    return true;
}

REGISTER_RESOURCE(UseSubR);

} // namespace sql
