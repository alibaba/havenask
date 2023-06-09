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
#include "indexlib/table/table_writer.h"

#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/table/building_segment_reader.h"

using namespace std;

namespace indexlib { namespace table {
IE_LOG_SETUP(table, TableWriter);

TableWriter::TableWriter() {}

TableWriter::~TableWriter() {}

bool TableWriter::Init(const TableWriterInitParamPtr& initParam)
{
    mRootDirectory = initParam->rootDirectory;
    mSegmentData = initParam->newSegmentData;
    mSegmentDataDirectory = initParam->segmentDataDirectory;
    mTableResource = initParam->tableResource;
    mSchema = initParam->schema;
    mOptions = initParam->options;
    mOnDiskVersion = initParam->onDiskVersion;
    mMemoryController = initParam->memoryController;
    mPartitionRange = initParam->partitionRange;
    bool initSuccess = DoInit();
    if (initSuccess) {
        mBuildingSegmentReader = CreateBuildingSegmentReader(mMemoryController);
        UpdateMemoryUse();
    }
    return initSuccess;
}

void TableWriter::UpdateMemoryUse()
{
    int64_t currentUse = GetCurrentMemoryUse();
    int64_t allocateMemUse = currentUse - mMemoryController->GetUsedQuota();
    if (allocateMemUse > 0) {
        mMemoryController->Allocate(allocateMemUse);
    } else {
        mMemoryController->Free(-allocateMemUse);
    }
}

bool TableWriter::IsFull() const { return false; }

segmentid_t TableWriter::GetBuildingSegmentId() const
{
    if (mSegmentData) {
        return mSegmentData->GetSegmentId();
    }
    return INVALID_SEGMENTID;
}

BuildingSegmentReaderPtr
TableWriter::CreateBuildingSegmentReader(const util::UnsafeSimpleMemoryQuotaControllerPtr& memoryController)
{
    return BuildingSegmentReaderPtr();
}
}} // namespace indexlib::table
