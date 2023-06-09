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

#include "autil/Log.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"

namespace indexlibv2::table {

class PrepareIndexReclaimParamOperation : public framework::IndexOperation
{
public:
    PrepareIndexReclaimParamOperation(const framework::IndexOperationDescription& desc);
    ~PrepareIndexReclaimParamOperation();

public:
    Status Execute(const framework::IndexTaskContext& context) override;
    static void GetOpInfo(const framework::IndexOperationDescription::Parameters& params,
                          framework::IndexOperationId& opId, bool& useOpFenceDir);
    static constexpr char OPERATION_TYPE[] = "prepare_index_reclaim_param_operation";
    static constexpr char TASK_STOP_TS_IN_SECONDS[] = "task_stop_ts_in_seconds";
    static constexpr char DOC_RECLAIM_DATA_DIR[] = "doc_reclaim_data";
    static constexpr char DOC_RECLAIM_CONF_FILE[] = "reclaim_config";

private:
    static constexpr char OP_ID[] = "operation_id";

private:
    framework::IndexOperationDescription _desc;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
