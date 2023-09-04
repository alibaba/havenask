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
#include "indexlib/index/ann/aitheta2/util/parallel_merge/ParallelMergeUtil.h"
using namespace std;
using namespace indexlib::file_system;
namespace indexlibv2::index::ann {

void ParallelMergeUtil::GetParallelMergeDirs(const indexlib::file_system::DirectoryPtr& directory,
                                             const ParallelReduceMeta& parallelReduceMeta,
                                             std::vector<indexlib::file_system::DirectoryPtr>& directories)
{
    for (uint32_t parallelNo = 0u; parallelNo < parallelReduceMeta.parallelCount; ++parallelNo) {
        std::string insDirectoryName = const_cast<ParallelReduceMeta&>(parallelReduceMeta).GetInsDirName(parallelNo);
        indexlib::file_system::DirectoryPtr insDirectory = directory->GetDirectory(insDirectoryName, true);
        AUTIL_LOG(INFO, "add parallel merge sub directory: [%s]", insDirectory->DebugString().c_str());
        directories.push_back(insDirectory);
    }
}

AUTIL_LOG_SETUP(indexlib.index, ParallelMergeUtil);
} // namespace indexlibv2::index::ann
