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
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"

namespace indexlibv2::table {

class PatchSegmentMoveOperation : public framework::IndexOperation
{
public:
    PatchSegmentMoveOperation(const framework::IndexOperationDescription& desc);
    ~PatchSegmentMoveOperation();

public:
    static constexpr char OPERATION_TYPE[] = "patch_segment_move";
    static constexpr char PARAM_PATCH_SCHEMA_ID[] = "patch_schema_id";
    static constexpr char PARAM_TARGET_SEGMENT_DIR[] = "target_segment_dir";
    static constexpr char PARAM_INDEX_OPERATIONS[] = "index_operations";

public:
    Status Execute(const framework::IndexTaskContext& context) override;

    static framework::IndexOperationDescription
    CreateOperationDescription(framework::IndexOperationId id, const std::string& targetSegmentName,
                               schemaid_t targetSchemaId,
                               const std::vector<framework::IndexOperationId>& dependAddIndexOperaions);

private:
    Status MoveIndexDir(framework::IndexOperationId opId, schemaid_t targetSchemaId, const std::string& segmentName,
                        const framework::IndexTaskContext& context);

private:
    framework::IndexOperationDescription _desc;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
