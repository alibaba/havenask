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
#include "indexlib_plugin/plugins/aitheta/aitheta_index_segment_retriever.h"

#include <algorithm>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "fslib/fslib.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include <proxima/common/params_define.h>

#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/index_segment_reader.h"
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "indexlib_plugin/plugins/aitheta/util/input_storage.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"

using namespace std;
using namespace aitheta;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, AithetaIndexSegmentRetriever);

size_t AithetaIndexSegmentRetriever::EstimateLoadMemoryUse(const indexlib::file_system::DirectoryPtr &dir) const {
    SegmentMeta sgtMeta;
    if (!sgtMeta.Load(dir)) {
        return 0l;
    }

    size_t size = 0ul;
    switch (sgtMeta.GetType()) {
        case SegmentType::kIndex: {
            size = dir->GetFileLength(INDEX_FILE) * IDXSEG_LOAD_MEM_USE_MAGIC_NUM;
        } break;
        case SegmentType::kPMIndex: {
            ParallelReduceMeta meta;
            if (!meta.Load(dir)) {
                IE_LOG(ERROR, "failed to load meta, return 0");
                return 0ul;
            }
            for (size_t i = 0ul; i < meta.parallelCount; ++i) {
                auto subDir = dir->MakeDirectory(meta.GetInsDirName(i));
                if (subDir) {
                    size += EstimateLoadMemoryUse(subDir);
                } else {
                    IE_LOG(ERROR, "failed to make subdir[%s]", meta.GetInsDirName(i).c_str());
                    return 0ul;
                }
            }
        } break;
        default: {
            IE_LOG(ERROR, "unexpected segment type[%s]", Type2Str(sgtMeta.GetType()).c_str());
            return 0ul;
        }
    }

    IE_LOG(INFO, "load memory size:[%f]MB with dir[%s]", size / 1048576.0f, dir->DebugString().c_str());
    return size;
}

MatchInfo AithetaIndexSegmentRetriever::Search(const std::string &query, autil::mem_pool::Pool *sessionPool) {
    INDEXLIB_FATAL_ERROR(UnImplement, "temp not support index segment retriever search");
    return MatchInfo();
}

bool AithetaIndexSegmentRetriever::DoOpen(const file_system::DirectoryPtr &dir) {
    SegmentMeta meta;
    if (!meta.Load(dir)) {
        IE_LOG(INFO, "failed to load segment meta in dir[%s]", dir->DebugString().c_str());
        return true;
    }

    const SegmentType &type = meta.GetType();
    if (SegmentType::kIndex == type || SegmentType::kPMIndex == type) {
        IndexSegmentPtr segment(new IndexSegment(meta));
        if (!segment->Load(dir, LoadType::kToSearch)) {
            return false;
        }
        mSgtReader.reset(new IndexSegmentReader(mParameters, segment));
    } else {
        IE_LOG(ERROR, "unexpected segment type[%s]", Type2Str(type).c_str());
        return false;
    }

    IE_LOG(INFO, "create segment reader with dir[%s]", dir->DebugString().c_str());
    return true;
}

}
}
