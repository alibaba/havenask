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
#include "indexlib_plugin/plugins/aitheta/util/parallel_merge/parallel_merge_util.h"
#include "indexlib_plugin/plugins/aitheta/index/index_attr.h"
using namespace std;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, ParallelMergeUtil);

bool ParallelMergeUtil::DumpSegmentMeta(const indexlib::file_system::DirectoryPtr &dir,
                                        const indexlib::index::ParallelReduceMeta &parallelReduceMeta) {
    SegmentMeta meta(SegmentType::kPMIndex);

    for (size_t parallelNo = 0u; parallelNo < parallelReduceMeta.parallelCount; ++parallelNo) {
        DirectoryPtr subDir =
            dir->GetDirectory(const_cast<ParallelReduceMeta &>(parallelReduceMeta).GetInsDirName(parallelNo), false);
        if (!subDir) {
            IE_LOG(ERROR, "failed to get sub-Directory, parallelNo[%lu]", parallelNo);
            return false;
        }
        SegmentMeta subMeta;
        if (!subMeta.Load(subDir)) {
            IE_LOG(ERROR, "failed to load segment meta, parallelNo[%lu]", parallelNo);
            return false;
        }

        if (0u == parallelNo) {
            int32_t dimension = subMeta.GetDimension();
            meta.SetDimension(dimension);
        }
        meta.Merge(subMeta);
    }

    if (!meta.Dump(dir)) {
        return false;
    }

    IE_LOG(INFO, "finish dump meta of parallel merge index segment");
    return true;
}

bool ParallelMergeUtil::DumpOfflineIndexAttr(const indexlib::file_system::DirectoryPtr &dir,
                                             const indexlib::index::ParallelReduceMeta &parallelReduceMeta) {
    OfflineIndexAttrHolder holder;
    for (size_t parallelNo = 0u; parallelNo < parallelReduceMeta.parallelCount; ++parallelNo) {
        DirectoryPtr subDir =
            dir->GetDirectory(const_cast<ParallelReduceMeta &>(parallelReduceMeta).GetInsDirName(parallelNo), false);
        if (!subDir) {
            IE_LOG(ERROR, "failed to get sub-Directory, parallelNo[%lu]", parallelNo);
            return false;
        }
        OfflineIndexAttrHolder subHolder;
        if (!subHolder.Load(subDir)) {
            IE_LOG(ERROR, "failed to load segment meta, parallelNo[%lu]", parallelNo);
            return false;
        }
        for (size_t idx = 0u; idx < subHolder.Size(); ++idx) {
            holder.Add(subHolder.Get(idx));
        }
    }

    if (!holder.Dump(dir)) {
        return false;
    }

    IE_LOG(INFO, "finish dump offline index attr of parallel merge index segment");
    return true;
}

bool ParallelMergeUtil::LoadIndexSegments(const indexlib::file_system::DirectoryPtr &dir,
                                          const indexlib::index::ParallelReduceMeta &parallelReduceMeta,
                                          const LoadType &loadType, std::vector<IndexSegmentPtr> &subIndexSgts) {
    subIndexSgts.clear();
    for (uint32_t idx = 0u; idx < parallelReduceMeta.parallelCount; ++idx) {
        std::string dirName = const_cast<ParallelReduceMeta &>(parallelReduceMeta).GetInsDirName(idx);
        DirectoryPtr subDir = dir->GetDirectory(dirName, true);

        SegmentMeta subMeta;
        if (!subMeta.Load(subDir)) {
            return false;
        }
        IndexSegmentPtr subSgt(new IndexSegment(subMeta));
        if (!subSgt->Load(subDir, loadType)) {
            return false;
        }
        subIndexSgts.push_back(subSgt);
    }
    return true;
}

}
}
