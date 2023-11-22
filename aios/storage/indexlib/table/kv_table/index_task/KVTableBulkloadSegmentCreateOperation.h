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

class KVTableBulkloadSegmentCreateOperation : public framework::IndexOperation
{
public:
    static constexpr char OPERATION_TYPE[] = "bulkload_segment_create";

    explicit KVTableBulkloadSegmentCreateOperation(const framework::IndexOperationDescription& desc);
    ~KVTableBulkloadSegmentCreateOperation() = default;

    Status Execute(const framework::IndexTaskContext& context) override;
    static framework::IndexOperationDescription CreateOperationDescription(framework::IndexOperationId opId);

private:
    Status GetOrMakeOutputDirectory(const framework::IndexTaskContext& context, const std::string& targetPath,
                                    std::shared_ptr<indexlib::file_system::IDirectory>* outputDir);

    framework::IndexOperationDescription _desc;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
