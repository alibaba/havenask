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

#include <ext/alloc_traits.h>
#include <functional>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileInfo.h"

namespace indexlib { namespace file_system {
class IDirectory;

class IndexFileList : public autil::legacy::Jsonizable
{
public:
    IndexFileList() noexcept = default;
    ~IndexFileList() noexcept = default;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) noexcept(false) override;
    void FromString(const std::string& indexMetaStr) noexcept(false);
    std::string ToString() const noexcept(false);

    bool Load(const std::shared_ptr<IDirectory>& directory, const std::string& fileName, bool* exist = NULL) noexcept;

    FSResult<int64_t> Load(const std::string& path) noexcept; // return ec & fileLength

    void Append(const FileInfo& fileMeta) noexcept;
    void AppendFinal(const FileInfo& fileMeta) noexcept;

    void Sort() noexcept;
    void Dedup() noexcept;
    // be equivalent to self = set(lhs) - set(rhs)
    // ATTENTION: lhs and rhs will change by sort
    void FromDifference(IndexFileList& lhs, IndexFileList& rhs) noexcept;

    void Filter(std::function<bool(const FileInfo&)> predicate) noexcept;

    bool operator==(const IndexFileList& other) const
    {
        // ATTENTION: order matters
        if (deployFileMetas.size() != other.deployFileMetas.size()) {
            return false;
        }
        if (finalDeployFileMetas.size() != other.finalDeployFileMetas.size()) {
            return false;
        }
        if (lifecycle != other.lifecycle) {
            return false;
        }
        for (size_t i = 0; i < deployFileMetas.size(); i++) {
            if (deployFileMetas[i] != other.deployFileMetas[i]) {
                return false;
            }
        }
        for (size_t i = 0; i < finalDeployFileMetas.size(); i++) {
            if (finalDeployFileMetas[i] != other.finalDeployFileMetas[i]) {
                return false;
            }
        }
        return true;
    }

    bool operator!=(const IndexFileList& other) const { return !(operator==(other)); }

public:
    // (string)filePath, (int64_t)fileLength, (uint64_t)modifyTime
    typedef std::vector<FileInfo> FileInfoVec;
    FileInfoVec deployFileMetas;
    FileInfoVec finalDeployFileMetas;
    std::string lifecycle;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexFileList> IndexFileListPtr;
}} // namespace indexlib::file_system
