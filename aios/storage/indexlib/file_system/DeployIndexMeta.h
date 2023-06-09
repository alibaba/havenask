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
#include <string>
#include <vector>

#include "indexlib/file_system/IndexFileList.h"

namespace indexlib { namespace file_system {

class DeployIndexMeta : public IndexFileList
{
public:
    // refer to raw index root pointing to the directory where index is produced.
    std::string sourceRootPath;
    // LOCAL: TABLE/GENERATION/PARTITION/[targetRootPath]/..., but dp2 does not support until now, so always empty
    // REMOTE: ABSOLUTE remote path, targetRootPath = mpangu://xx/... or dcache://xx/... or pangu://xx/..., empty means
    // indexRoot in suez target
    std::string targetRootPath;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) noexcept(false) override
    {
        IndexFileList::Jsonize(json);
        json.Jsonize("source_root_path", sourceRootPath, sourceRootPath);
        json.Jsonize("target_root_path", targetRootPath, targetRootPath);
    }

    bool operator==(const DeployIndexMeta& other) const
    {
        return sourceRootPath == other.sourceRootPath && targetRootPath == other.targetRootPath &&
               IndexFileList::operator==(other);
    }
};

// vector for multi src root
typedef std::vector<std::shared_ptr<DeployIndexMeta>> DeployIndexMetaVec;

typedef std::shared_ptr<IndexFileList> IndexFileListPtr;
}} // namespace indexlib::file_system
