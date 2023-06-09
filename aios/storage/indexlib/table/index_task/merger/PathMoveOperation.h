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

/* attention: this op move src path to dest, when the path to move is not exist in source,
   we think it's failover scene, do nothing!
*/

class PathMoveOperation : public framework::IndexOperation
{
public:
    static constexpr char OPERATION_TYPE[] = "path_move";
    static constexpr char PARAM_OP_TO_MOVE_PATHS[] = "op_to_move_src_paths";

public:
    PathMoveOperation(const framework::IndexOperationDescription& desc)
        : framework::IndexOperation(desc.GetId(), desc.UseOpFenceDir())
        , _desc(desc)
    {
    }

    ~PathMoveOperation() = default;

public:
    static framework::IndexOperationDescription
    CreateOperationDescription(framework::IndexOperationId opId,
                               const std::map<std::string, framework::IndexOperationId>& movePaths);

public:
    Status Execute(const framework::IndexTaskContext& context) override;

private:
    framework::IndexOperationDescription _desc;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
