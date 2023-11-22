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

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/open_executor/executor_resource.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/partition/patch_loader.h"

DECLARE_REFERENCE_CLASS(util, MemoryReserver);

namespace indexlib { namespace partition {

class ReopenPartitionReaderExecutor : public OpenExecutor
{
public:
    ReopenPartitionReaderExecutor(bool hasPreload, const std::string& partitionName = "")
        : OpenExecutor(partitionName)
        , mHasPreload(hasPreload)
    {
    }
    virtual ~ReopenPartitionReaderExecutor();

    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override;

private:
    bool CanLoadReader(ExecutorResource& resource, const PatchLoaderPtr& patchLoader,
                       const util::MemoryReserverPtr& memReserver);

private:
    bool mHasPreload;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReopenPartitionReaderExecutor);
}} // namespace indexlib::partition
