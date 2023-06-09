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
#ifndef INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_PARALLEL_MERGE_TASK_PARALLEL_MERGE_UTIL_H
#define INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_PARALLEL_MERGE_TASK_PARALLEL_MERGE_UTIL_H

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/segment_meta.h"
#include "indexlib_plugin/plugins/aitheta/index_segment.h"
namespace indexlib {
namespace aitheta_plugin {

struct ParallelMergeUtil {
    static bool DumpSegmentMeta(const indexlib::file_system::DirectoryPtr &directory,
                                const indexlib::index::ParallelReduceMeta &parallelReduceMeta);
    static bool DumpOfflineIndexAttr(const indexlib::file_system::DirectoryPtr &directory,
                                     const indexlib::index::ParallelReduceMeta &parallelReduceMeta);

    static bool LoadIndexSegments(const indexlib::file_system::DirectoryPtr &directory,
                                  const indexlib::index::ParallelReduceMeta &parallelReduceMeta,
                                  const LoadType &loadType, std::vector<IndexSegmentPtr> &subIndexSgts);
 private:
    IE_LOG_DECLARE();
};

}
}

#endif  // INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_PARALLEL_MERGE_TASK_PARALLEL_MERGE_UTIL_H