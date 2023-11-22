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
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);

namespace indexlib { namespace file_system {

typedef std::vector<DirectoryPtr> DirectoryVector;
}} // namespace indexlib::file_system

namespace indexlib { namespace index_base {

class SegmentDirectoryCreator
{
public:
    SegmentDirectoryCreator();
    ~SegmentDirectoryCreator();

public:
    static SegmentDirectoryPtr Create(const config::IndexPartitionOptions& options,
                                      const file_system::DirectoryPtr& directory,
                                      const index_base::Version& version = index_base::Version(INVALID_VERSIONID),
                                      bool hasSub = false);

    static SegmentDirectoryPtr Create(const file_system::DirectoryPtr& dir,
                                      index_base::Version version = index_base::Version(INVALID_VERSIONID),
                                      const config::IndexPartitionOptions* options = NULL, bool hasSub = false);

    static SegmentDirectoryPtr Create(const file_system::DirectoryVector& directoryVec, bool hasSub = false);

    static SegmentDirectoryPtr Create(const file_system::DirectoryVector& directoryVec,
                                      const std::vector<index_base::Version>& versions, bool hasSub = false);

private:
    static SegmentDirectoryPtr Create(const config::IndexPartitionOptions* options, bool isParallelBuild = false);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDirectoryCreator);
}} // namespace indexlib::index_base
