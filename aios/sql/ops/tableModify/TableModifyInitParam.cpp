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
#include "sql/ops/tableModify/TableModifyInitParam.h"

#include <engine/NaviConfigContext.h>
#include <iosfwd>

#include "autil/legacy/exception.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/log/NaviLogger.h"
#include "sql/ops/util/KernelUtil.h"

using namespace std;

namespace sql {

bool TableModifyInitParam::initFromJson(navi::KernelConfigContext &ctx) {
    try {
        NAVI_JSONIZE(ctx, "catalog_name", catalogName);
        NAVI_JSONIZE(ctx, "db_name", dbName);
        NAVI_JSONIZE(ctx, "table_name", tableName);
        NAVI_JSONIZE(ctx, "table_type", tableType);
        NAVI_JSONIZE(ctx, "pk_field", pkField);
        NAVI_JSONIZE(ctx, "operation", operation);
        NAVI_JSONIZE(ctx, "table_distribution", tableDist, tableDist);
        ctx.JsonizeAsString("condition", conditionJson, "");
        ctx.JsonizeAsString("modify_field_exprs", modifyFieldExprsJson, "");
        NAVI_JSONIZE(ctx, "output_fields", outputFields, outputFields);
        NAVI_JSONIZE(ctx, "output_fields_type", outputFieldsType, outputFieldsType);
        if (!tableDist.hashMode.validate()) {
            NAVI_KERNEL_LOG(ERROR, "validate hash mode failed.");
            return false;
        }
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_KERNEL_LOG(ERROR, "init failed error:[%s].", e.what());
        return false;
    } catch (...) { return false; }
    stripName();
    return true;
}

void TableModifyInitParam::stripName() {
    KernelUtil::stripName(pkField);
    KernelUtil::stripName(tableDist.hashMode._hashFields);
    KernelUtil::stripName(outputFields);
}

} // namespace sql
