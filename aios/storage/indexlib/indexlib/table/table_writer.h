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
#ifndef __INDEXLIB_TABLE_WRITER_H
#define __INDEXLIB_TABLE_WRITER_H

#include <memory>

#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/document.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/table_common.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, SegmentDataBase);
DECLARE_REFERENCE_CLASS(table, TableResource);
DECLARE_REFERENCE_CLASS(table, BuildingSegmentReader);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace table {

class TableWriter
{
public:
    enum class BuildResult { BR_OK = 0, BR_SKIP = 1, BR_FAIL = 2, BR_NO_SPACE = 3, BR_FATAL = 4, BR_UNKNOWN = -1 };

public:
    TableWriter();
    virtual ~TableWriter();

public:
    bool Init(const TableWriterInitParamPtr& initParam);

    virtual bool DoInit() = 0;
    virtual BuildResult Build(docid_t docId, const document::DocumentPtr& doc) = 0;
    virtual bool IsDirty() const = 0;
    virtual bool DumpSegment(BuildSegmentDescription& segmentDescription) = 0;
    virtual BuildingSegmentReaderPtr
    CreateBuildingSegmentReader(const util::UnsafeSimpleMemoryQuotaControllerPtr& memoryController);
    virtual bool IsFull() const;
    virtual size_t GetLastConsumedMessageCount() const = 0;

public:
    segmentid_t GetBuildingSegmentId() const;
    BuildingSegmentReaderPtr GetBuildingSegmentReader() const { return mBuildingSegmentReader; }
    void UpdateMemoryUse();
    index_base::Version GetOnDiskVersion() { return mOnDiskVersion; }

public:
    virtual size_t EstimateInitMemoryUse(const TableWriterInitParamPtr& initParam) const = 0;
    virtual size_t GetCurrentMemoryUse() const = 0;
    virtual size_t EstimateDumpMemoryUse() const = 0;
    virtual size_t EstimateDumpFileSize() const = 0;

protected:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

protected:
    file_system::DirectoryPtr mRootDirectory;
    file_system::DirectoryPtr mSegmentDataDirectory;
    index_base::SegmentDataBasePtr mSegmentData;
    index_base::Version mOnDiskVersion;
    TableResourcePtr mTableResource;
    PartitionRange mPartitionRange;
    util::UnsafeSimpleMemoryQuotaControllerPtr mMemoryController;

private:
    BuildingSegmentReaderPtr mBuildingSegmentReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableWriter);
}} // namespace indexlib::table

#endif //__INDEXLIB_TABLE_WRITER_H
