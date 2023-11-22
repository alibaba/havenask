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
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/index_partition.h"

namespace indexlib::partition {

class PartitionValidater
{
public:
    PartitionValidater(const config::IndexPartitionOptions& options);
    ~PartitionValidater();

    PartitionValidater(const PartitionValidater&) = delete;
    PartitionValidater& operator=(const PartitionValidater&) = delete;
    PartitionValidater(PartitionValidater&&) = delete;
    PartitionValidater& operator=(PartitionValidater&&) = delete;

public:
    bool Init(const std::string& indexPath, versionid_t versionId = INVALID_VERSIONID);
    void Check();

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    IndexPartitionPtr mPartition;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::partition
