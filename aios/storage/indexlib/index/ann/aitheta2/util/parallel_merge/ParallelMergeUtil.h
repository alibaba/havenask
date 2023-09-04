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

#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/parallel_merge/ParallelReduceMeta.h"
namespace indexlibv2::index::ann {

struct ParallelMergeUtil {
    static void GetParallelMergeDirs(const indexlib::file_system::DirectoryPtr& directory,
                                     const ParallelReduceMeta& parallelReduceMeta,
                                     std::vector<indexlib::file_system::DirectoryPtr>& directories);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann