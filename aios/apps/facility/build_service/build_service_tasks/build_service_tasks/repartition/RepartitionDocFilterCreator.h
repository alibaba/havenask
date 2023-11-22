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

#include "build_service/common_define.h"
#include "build_service/config/HashModeConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/merger/doc_filter.h"
#include "indexlib/merger/doc_filter_creator.h"
#include "indexlib/merger/segment_directory.h"

namespace build_service { namespace task_base {

class RepartitionDocFilterCreator : public indexlib::merger::DocFilterCreator
{
public:
    RepartitionDocFilterCreator(const proto::Range& partitionRange, const config::HashModeConfigPtr& hashModeConfig);
    ~RepartitionDocFilterCreator();

private:
    RepartitionDocFilterCreator(const RepartitionDocFilterCreator&);
    RepartitionDocFilterCreator& operator=(const RepartitionDocFilterCreator&);

public:
    indexlib::merger::DocFilterPtr CreateDocFilter(const indexlib::merger::SegmentDirectoryPtr& segDir,
                                                   const indexlib::config::IndexPartitionSchemaPtr& schema,
                                                   const indexlib::config::IndexPartitionOptions& options) override;

private:
    proto::Range _partitionRange;
    config::HashModeConfigPtr _hashModeConfig;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RepartitionDocFilterCreator);

}} // namespace build_service::task_base
