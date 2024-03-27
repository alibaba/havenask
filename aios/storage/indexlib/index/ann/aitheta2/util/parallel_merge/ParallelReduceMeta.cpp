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

#include "indexlib/index/ann/aitheta2/util/parallel_merge/ParallelReduceMeta.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlibv2::index::ann {
AUTIL_LOG_SETUP(indexlib.index, ParallelReduceMeta);

void ParallelReduceMeta::Store(const DirectoryPtr& directory) const
{
    string fileName = GetFileName();
    indexlib::file_system::RemoveOption removeOption = RemoveOption::MayNonExist();
    directory->RemoveFile(fileName, removeOption);
    directory->Store(fileName, ToJsonString(*this));
}

bool ParallelReduceMeta::Load(const DirectoryPtr& directory)
{
    string fileName = GetFileName();
    string content;
    try {
        if (!directory->IsExist(fileName)) {
            return false;
        }
        directory->Load(fileName, content);
        FromJsonString(*this, content);
    } catch (...) {
        AUTIL_LOG(WARN, "load parallel meta from [%s] failed.", directory->DebugString().c_str());
        return false;
    }

    return true;
}

string ParallelReduceMeta::GetInsDirName(uint32_t parallelId)
{
    // this method is from ParallelMergeItem::GetParallelInstanceDirName
    if (parallelCount > 1 && parallelId >= 0) {
        return "inst_" + StringUtil::toString(parallelCount) + "_" + StringUtil::toString(parallelId);
    }
    return string("");
}

} // namespace indexlibv2::index::ann