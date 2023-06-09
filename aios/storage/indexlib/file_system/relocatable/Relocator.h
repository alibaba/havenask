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

#include "autil/Log.h"
#include "indexlib/base/Status.h"

namespace indexlib::util {
class MetricProvider;

}

namespace indexlib::file_system {

class EntryTable;
class RelocatableFolder;

class Relocator
{
public:
    static std::pair<Status, std::shared_ptr<RelocatableFolder>>
    CreateFolder(const std::string& folderRoot, const std::shared_ptr<util::MetricProvider>& metricProvider);
    static Status SealFolder(const std::shared_ptr<RelocatableFolder>& folder);

public:
    Relocator(const std::string& targetDir) : _targetDir(targetDir) {}
    virtual ~Relocator() {}

    Status AddSource(const std::string& sourceDir);
    Status Relocate();

protected:
    Status RelocateDirectory(const EntryTable& entryTable, const std::string& relativePath);
    virtual Status MakeDirectory(const std::string& relativePath);
    virtual Status RelocateFile(const std::string& sourceRoot, const std::string& relativePath);

private:
    std::string _targetDir;
    std::vector<std::string> _sourceDirs;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::file_system
