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
#include "indexlib/table/index_task/merger/SegmentMergePlan.h"

namespace indexlibv2::table {
class SingleSegmentDocumentGroupSelector;
class ReclaimMapOperation : public framework::IndexOperation
{
public:
    ReclaimMapOperation(const framework::IndexOperationDescription& description);
    ~ReclaimMapOperation();

    Status Execute(const framework::IndexTaskContext& context) override;

public:
    static constexpr char OPERATION_TYPE[] = "reclaim_map";

private:
    std::pair<Status, std::shared_ptr<indexlib::file_system::Directory>>
    GetPatchDir(const framework::IndexTaskContext& context, const framework::IndexOperationId& opLog2PatchOperationId);

private:
    framework::IndexOperationDescription _description;
    std::string _compactionType;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
