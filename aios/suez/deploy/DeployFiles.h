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

#include <string>
#include <vector>

#include "autil/HashUtil.h"
#include "worker_framework/DataItem.h"

namespace suez {

struct DeployFiles {
    std::string sourceRootPath; // the root where file produced, refer to raw index root, maybe pangu, hdfs
    std::string
        targetRootPath; // the root where file load, refer to local toot or index root, maybe local, mpangu, dcache...
    std::vector<std::string> deployFiles;
    std::vector<worker_framework::DataFileMeta> srcFileMetas;
    int64_t deploySize = 0;
    bool isComplete = false;
};

typedef std::vector<DeployFiles> DeployFilesVec;

inline bool operator==(const suez::DeployFiles &left, const suez::DeployFiles &right) {
    if (left.sourceRootPath != right.sourceRootPath) {
        return false;
    }
    if (left.targetRootPath != right.targetRootPath) {
        return false;
    }
    if (left.deployFiles != right.deployFiles) {
        return false;
    }
    return true;
}

inline bool operator!=(const suez::DeployFiles &left, const suez::DeployFiles &right) { return !(left == right); }

} // namespace suez

template <>
struct std::hash<suez::DeployFiles> {
    std::size_t operator()(const suez::DeployFiles &deployFiles) const noexcept {
        size_t seed = 0;
        autil::HashUtil::combineHash(seed, deployFiles.sourceRootPath);
        autil::HashUtil::combineHash(seed, deployFiles.targetRootPath);
        for (const auto &v : deployFiles.deployFiles) {
            autil::HashUtil::combineHash(seed, v);
        }
        return seed;
    }
};

template <>
struct std::hash<suez::DeployFilesVec> {
    std::size_t operator()(const suez::DeployFilesVec &deployFilesVec) const noexcept {
        size_t seed = 0;
        for (const auto &v : deployFilesVec) {
            autil::HashUtil::combineHash(seed, v);
        }
        return seed;
    }
};
