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
#ifndef __INDEXLIB_OPEN_EXECUTOR_UTIL_H
#define __INDEXLIB_OPEN_EXECUTOR_UTIL_H

#include "indexlib/common_define.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/open_executor/executor_resource.h"
#include "indexlib/partition/patch_loader.h"

namespace indexlib { namespace partition {

class OpenExecutorUtil
{
public:
    OpenExecutorUtil();
    ~OpenExecutorUtil();

public:
    static PartitionModifierPtr CreateInplaceModifier(const config::IndexPartitionOptions& options,
                                                      config::IndexPartitionSchemaPtr schema,
                                                      const index_base::PartitionDataPtr& partData);

    /* reader should be hold to avoid mmap file node clean by cleaner*/
    static OnlinePartitionReaderPtr GetPatchedReader(ExecutorResource& resource,
                                                     const index_base::PartitionDataPtr& partData,
                                                     const PatchLoaderPtr& patchLoader,
                                                     const std::string& partitionName,
                                                     const OnlinePartitionReader* hintReader = nullptr);

    static void InitReader(ExecutorResource& resource, const index_base::PartitionDataPtr& partData,
                           const PatchLoaderPtr& patchLoader, const std::string& partitionName,
                           const OnlinePartitionReader* hintReader = nullptr);

    static void InitWriter(ExecutorResource& resource, const index_base::PartitionDataPtr& newPartitionData);
    static size_t EstimateReopenMemoryUse(ExecutorResource& resource, const std::string& partitionName);

    static PatchLoaderPtr CreatePatchLoader(const ExecutorResource& resource,
                                            const index_base::PartitionDataPtr& partitionData,
                                            const std::string& partitionName, bool ignorePatchToOldIncSegment);

private:
    static void SwitchReader(ExecutorResource& resource, const OnlinePartitionReaderPtr& reader);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OpenExecutorUtil);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OPEN_EXECUTOR_UTIL_H
