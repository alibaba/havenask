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
#include "sql/ops/calc/CalcInitParamR.h"

#include <algorithm>
#include <engine/NaviConfigContext.h>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/ops/util/KernelUtil.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string CalcInitParamR::RESOURCE_ID = "calc_init_param_r";

void CalcInitParamR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool CalcInitParamR::config(navi::ResourceConfigContext &ctx) {
    if (ctx.hasKey("output_fields_internal")) {
        NAVI_JSONIZE(ctx, "output_fields_internal", outputFields);
        NAVI_JSONIZE(ctx, "output_fields_internal_type", outputFieldsType, outputFieldsType);
    } else {
        NAVI_JSONIZE(ctx, "output_fields", outputFields);
        NAVI_JSONIZE(ctx, "output_fields_type", outputFieldsType, outputFieldsType);
    }
    bool ok = false;
    if (ctx.hasKey("push_down_ops")) {
        auto pushDownOpsCtx = ctx.enter("push_down_ops");
        if (!pushDownOpsCtx.isArray()) {
            SQL_LOG(ERROR, "push down ops not array.");
        }
        if (pushDownOpsCtx.size() > 0) {
            auto firstOpCtx = pushDownOpsCtx.enter(0);
            std::string opName;
            NAVI_JSONIZE(firstOpCtx, "op_name", opName);
            if (opName == "CalcOp") {
                auto attrCtx = firstOpCtx.enter("attrs");
                attrCtx.JsonizeAsString("condition", conditionJson, "");
                attrCtx.JsonizeAsString("output_field_exprs", outputExprsJson, "");
                ok = true;
            }
        }
    }
    if (!ok) {
        ctx.JsonizeAsString("condition", conditionJson, "");
        ctx.JsonizeAsString("output_field_exprs", outputExprsJson, "");
    }
    outputFieldsOrigin = outputFields;
    KernelUtil::stripName(outputFields);
    if (outputExprsJson.empty()) {
        outputExprsJson = "{}";
    }
    return true;
}

navi::ErrorCode CalcInitParamR::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

REGISTER_RESOURCE(CalcInitParamR);

} // namespace sql
