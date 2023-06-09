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
#ifndef __INDEXLIB_ON_DISK_INDEX_CLEANER_H
#define __INDEXLIB_ON_DISK_INDEX_CLEANER_H

#include <memory>

#include "fslib/fslib.h"
#include "indexlib/common/executor.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, Version);

namespace indexlib { namespace partition {

// IndexCleaner is used to clean up index files whose versions are outside of KeepVersionCount.
class OnDiskIndexCleaner : public common::Executor
{
public:
    OnDiskIndexCleaner(uint32_t keepVersionCount, const ReaderContainerPtr& readerContainer,
                       const file_system::DirectoryPtr& directory);
    ~OnDiskIndexCleaner();

public:
    void Execute() override;

private:
    void DoExecute(const file_system::DirectoryPtr& rootDirectoy);
    void ConstructNeedKeepVersionIdxAndSegments(const file_system::DirectoryPtr& dir, const fslib::FileList& fileList,
                                                size_t& firstKeepVersionIdx, std::set<segmentid_t>& needKeepSegments,
                                                std::set<schemavid_t>& needKeepSchemaId);

    segmentid_t CalculateMaxSegmentIdInAllVersions(const file_system::DirectoryPtr& dir,
                                                   const fslib::FileList& fileList);

    void CleanVersionFiles(const file_system::DirectoryPtr& dir, size_t firstKeepVersionIdx,
                           const fslib::FileList& fileList);

    void CleanSegmentFiles(const file_system::DirectoryPtr& dir, segmentid_t maxSegIdInAllVersions,
                           const std::set<segmentid_t>& needKeepSegments);

    void CleanPatchIndexSegmentFiles(const file_system::DirectoryPtr& dir, segmentid_t maxSegInVersion,
                                     const std::set<segmentid_t>& needKeepSegment);

    void CleanUselessSchemaFiles(const file_system::DirectoryPtr& dir, const std::set<schemavid_t>& needKeepSchemaId);

private:
    uint32_t mKeepVersionCount;
    ReaderContainerPtr mReaderContainer;
    file_system::DirectoryPtr mDirectory;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskIndexCleaner);
}} // namespace indexlib::partition

#endif //__INDEXLIB_ON_DISK_INDEX_CLEANER_H
