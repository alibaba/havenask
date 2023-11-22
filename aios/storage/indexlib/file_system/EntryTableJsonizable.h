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
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/ErrorCode.h"

namespace indexlib { namespace file_system {
class EntryTable;

class EntryTableJsonizable : public autil::legacy::Jsonizable
{
public:
    EntryTableJsonizable();
    ~EntryTableJsonizable();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    ErrorCode CollectAll(const EntryTable* entryTable, const std::vector<std::string>& filterDirList);
    ErrorCode CollectFromEntryTable(const EntryTable* entryTable, const std::vector<std::string>& wishFileList,
                                    const std::vector<std::string>& wishDirList,
                                    const std::vector<std::string>& filterDirList);

public:
    struct PackageMetaInfo : public autil::legacy::Jsonizable {
        int64_t length = -1;
        // "files" -> {logicalPath -> EntryMeta}
        std::map<std::string, EntryMeta> files;

        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
        std::string DebugString() const;
    };

public:
    std::map<std::string, std::map<std::string, EntryMeta>>& GetFiles();
    std::map<std::string, std::map<std::string, PackageMetaInfo>>& GetPackageFiles();

private:
    ErrorCode CollectFile(const EntryTable* entryTable, const std::string& file);
    ErrorCode CollectDir(const EntryTable* entryTable, const std::string& dir,
                         std::unordered_set<std::string> filteredLogicPaths);
    ErrorCode CompletePackageFileLength(const EntryTable* entryTable, bool collectAll);
    ErrorCode CollectEntryMeta(const EntryTable* entryTable, const EntryMeta& entryMeta);
    ErrorCode CollectParentEntryMetas(const EntryTable* entryTable, const EntryMeta& entryMeta);

private:
    std::map<std::string, std::string> _packageFileRoots;
    // "files" -> {physicalRoot -> {logicalPath -> EntryMeta}}
    std::map<std::string, std::map<std::string, EntryMeta>> _files;
    // "package_files" -> {physicalRoot -> {packageFilePath -> PackageMetaInfo}}
    std::map<std::string, std::map<std::string, PackageMetaInfo>> _packageFiles;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<EntryTableJsonizable> EntryTableJsonizablePtr;
}} // namespace indexlib::file_system
