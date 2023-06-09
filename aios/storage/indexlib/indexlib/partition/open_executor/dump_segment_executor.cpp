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
#include "indexlib/partition/open_executor/dump_segment_executor.h"

#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/open_executor/open_executor_util.h"
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, DumpSegmentExecutor);

DumpSegmentExecutor::DumpSegmentExecutor(const string& partitionName, bool reInitReaderAndWriter)
    : OpenExecutor(partitionName)
    , mReInitReaderAndWriter(reInitReaderAndWriter)
    , mHasSkipInitReaderAndWriter(false)
{
}

DumpSegmentExecutor::~DumpSegmentExecutor() {}

bool DumpSegmentExecutor::Execute(ExecutorResource& resource)
{
    IE_LOG(INFO, "dump segment executor begin");
    mHasSkipInitReaderAndWriter = false;
    if (!resource.mWriter || resource.mWriter->IsClosed()) {
        return true;
    }

    if (!resource.mWriter->DumpSegmentWithMemLimit()) {
        return false;
    }

    PartitionDataPtr partData = resource.mPartitionDataHolder.Get();
    partData->CreateNewSegment();

    if (mReInitReaderAndWriter) {
        OpenExecutorUtil::InitReader(resource, partData, PatchLoaderPtr(), mPartitionName);
        PartitionModifierPtr modifier =
            PartitionModifierCreator::CreateInplaceModifier(resource.mSchema, resource.mReader);
        resource.mWriter->ReOpenNewSegment(modifier);
        resource.mReader->EnableAccessCountors(resource.mNeedReportTemperatureAccess);
    } else {
        mHasSkipInitReaderAndWriter = true;
        IE_LOG(INFO, "skip init reader and writer");
    }
    IE_LOG(INFO, "dump segment executor end");
    return true;
}

void DumpSegmentExecutor::Drop(ExecutorResource& resource)
{
    if (mHasSkipInitReaderAndWriter) {
        if (!resource.mWriter || resource.mWriter->IsClosed()) {
            return;
        }
        IE_LOG(INFO, "drop reinit reader and writer");
        PartitionDataPtr partData = resource.mPartitionDataHolder.Get();
        partData->CreateNewSegment();
        OpenExecutorUtil::InitReader(resource, partData, PatchLoaderPtr(), mPartitionName);
        PartitionModifierPtr modifier =
            PartitionModifierCreator::CreateInplaceModifier(resource.mSchema, resource.mReader);
        resource.mWriter->ReOpenNewSegment(modifier);
        resource.mReader->EnableAccessCountors(resource.mNeedReportTemperatureAccess);
    }
}
}} // namespace indexlib::partition
