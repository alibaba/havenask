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
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"

namespace indexlibv2::table {

class DropOpLogOperation : public framework::IndexOperation
{
public:
    static constexpr char OPERATION_TYPE[] = "drop_op_log_operation";
    static constexpr char SEGMENT_ID[] = "segment_id";
    static constexpr char TARGET_SCHEMA_ID[] = "target_schema_id";

public:
    DropOpLogOperation(const framework::IndexOperationDescription& desc);
    ~DropOpLogOperation();

public:
    static bool NeedDropOpLog(segmentid_t segmentId, const std::shared_ptr<config::TabletSchema>& baseSchema,
                              const std::shared_ptr<config::TabletSchema>& targetSchema);
    Status Execute(const framework::IndexTaskContext& context) override;

    static framework::IndexOperationDescription
    CreateOperationDescription(framework::IndexOperationId id, segmentid_t targetSegmentId, schemaid_t targetSchema);

private:
    static std::set<fieldid_t> CalculateDropFields(const std::shared_ptr<config::TabletSchema>& baseSchema,
                                                   const std::shared_ptr<config::TabletSchema>& targetSchema);

private:
    framework::IndexOperationDescription _desc;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
