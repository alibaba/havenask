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
#include "indexlib/common/executor.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);

namespace indexlib { namespace partition {

class PartitionReaderCleaner : public common::Executor
{
public:
    PartitionReaderCleaner(const ReaderContainerPtr& readerContainer, const file_system::IFileSystemPtr& fileSystem,
                           autil::RecursiveThreadMutex& dataLock, const std::string& partitionName = "");
    ~PartitionReaderCleaner();

public:
    void Execute() override;

private:
    ReaderContainerPtr mReaderContainer;
    file_system::IFileSystemPtr mFileSystem;
    autil::RecursiveThreadMutex& mDataLock;
    std::string mPartitionName;
    int64_t mLastTs;

private:
    friend class PartitionReaderCleanerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionReaderCleaner);
}} // namespace indexlib::partition
