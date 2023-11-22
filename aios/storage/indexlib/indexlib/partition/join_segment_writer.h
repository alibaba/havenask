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

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_cursor.h"

DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, ReopenOperationRedoStrategy);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(index, DeletionMapWriter);

namespace indexlib { namespace util {
template <typename T>
class SimpleMemoryQuotaControllerType;
typedef SimpleMemoryQuotaControllerType<std::atomic<int64_t>> SimpleMemoryQuotaController;
DEFINE_SHARED_PTR(SimpleMemoryQuotaController);
}} // namespace indexlib::util

namespace indexlib { namespace partition {

class JoinSegmentWriter
{
public:
    JoinSegmentWriter(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      const file_system::IFileSystemPtr& fileSystem,
                      const util::PartitionMemoryQuotaControllerPtr& memController);
    ~JoinSegmentWriter();

public:
    void Init(const index_base::PartitionDataPtr& partitionData, const index_base::Version& newVersion,
              const index_base::Version& lastVersion);
    bool PreJoin();
    bool Join();
    bool JoinDeletionMap();
    bool Dump(const index_base::PartitionDataPtr& writePartitionData);

private:
    /* void ReportRedoCountMetric(const index::OperationQueuePtr& opQueue); */
    index::DeletionMapWriterPtr GetDeletionMapWriter(const PartitionModifierPtr& modifier);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    file_system::IFileSystemPtr mFileSystem;
    util::SimpleMemoryQuotaControllerPtr mMemController;

    OperationCursor mOpCursor;
    index_base::PartitionDataPtr mReadPartitionData;
    index_base::PartitionDataPtr mNewPartitionData;
    index_base::Version mOnDiskVersion;
    index_base::Version mLastVersion;
    PartitionModifierPtr mModifier;
    int64_t mMaxTsInNewVersion;
    ReopenOperationRedoStrategyPtr mRedoStrategy;

private:
    friend class JoinSegmentWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinSegmentWriter);
}} // namespace indexlib::partition
