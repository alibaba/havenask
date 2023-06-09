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
#ifndef __INDEXLIB_SEGMENT_FILE_LIST_WRAPPER_H
#define __INDEXLIB_SEGMENT_FILE_LIST_WRAPPER_H

#include <memory>
#include <vector>

#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

struct SegmentInfo;
DEFINE_SHARED_PTR(SegmentInfo);

class SegmentFileListWrapper
{
private:
    SegmentFileListWrapper();
    ~SegmentFileListWrapper();

public:
    static bool Load(const file_system::DirectoryPtr& directory, file_system::IndexFileList& meta);

    // for directory
    static bool Dump(const file_system::DirectoryPtr& directory, const std::string& lifecycle);
    static bool Dump(const file_system::DirectoryPtr& directory, const std::vector<std::string>& fileList);
    static void Dump(const file_system::DirectoryPtr& directory, const SegmentInfoPtr& segmentInfo);
    static void Dump(const file_system::DirectoryPtr& directory, const fslib::FileList& fileList,
                     const SegmentInfoPtr& segmentInfo);

public:
    static bool TEST_Dump(const std::string& segmentPath, const fslib::FileList& fileList);

private:
    static bool needDumpLegacyFile();
    static bool IsExist(const file_system::DirectoryPtr& dir);
    static void DoDump(const file_system::DirectoryPtr& directory, const std::vector<file_system::FileInfo>& fileInfos,
                       const SegmentInfoPtr& segmentInfo, const std::string& lifecycle);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentFileListWrapper);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_SEGMENT_FILE_LIST_WRAPPER_H
