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

#include <map>
#include <set>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/ErrorCode.h"

namespace indexlib { namespace file_system {

struct FileSystemOptions;
class EntryMeta;
class EntryTable;

// TODO(panghai.hj): Handle set physical path correctly for merge.

// EntryTableMerger is used during file system/EntryTable merge phase, for example after multiple instances(each might
// have multiple threads) produce index files and file system mount the index files from different instance directories
// and merge the index files to final output format. There are IO operations(file rename etc) in this class that might
// throw exceptions. There are basically a few steps during the merge phase:
// 1. Physically move the files, dirs(both inside and outside of packages) from source dir to destination.
// 2. Rename package meta/data files. Before the merge, package meta/data files.
//    e.g. new_dir/package_file.__data__.i0t1.0 -> new_dir/package_file.__data__1
// 3. Update entry-table. The entry meta needs to be updated to have correct new logical/physical paths.
class EntryTableMerger
{
public:
    EntryTableMerger(std::shared_ptr<EntryTable> entryTable);
    ~EntryTableMerger();

public:
    ErrorCode Merge(bool inRecoverPhase = false);

private:
    ErrorCode RenamePackageMetaAndDataFiles();
    ErrorCode HandleMovePackageMetaFiles(const std::map<std::string, std::string>& movedPackageDataFiles,
                                         std::set<std::string>* destDirSet);
    ErrorCode MaybeHandleFile(bool inRecoverPhase, std::map<std::string, std::string>* movedPackageDataFiles);
    ErrorCode MaybeHandleDir(const std::set<std::string>& packageFileDstDirPhysicalPaths);

private:
    std::shared_ptr<EntryTable> _entryTable;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<EntryTableMerger> EntryTableMergerPtr;
}} // namespace indexlib::file_system
