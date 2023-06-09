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

#include "indexlib_plugin/plugins/aitheta/aitheta_index_reduce_item.h"

#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/Exception.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

#include "indexlib_plugin/plugins/aitheta/aitheta_indexer.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/index_segment.h"
#include "indexlib_plugin/plugins/aitheta/raw_segment.h"
#include "indexlib_plugin/plugins/aitheta/rt_segment.h"

using namespace std;

using namespace indexlib::file_system;
using namespace indexlib::index;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, AithetaIndexReduceItem);

bool AithetaIndexReduceItem::UpdateDocId(const DocIdMap &docIdMap) {
    if (!mSegment) {
        IE_LOG(ERROR, "mSegment is not initialized");
        return false;
    }
    mSegment->SetDocIdMap(docIdMap);
    return true;
}

bool AithetaIndexReduceItem::LoadIndex(const DirectoryPtr &indexDir) {
    SegmentMeta meta;
    if (!meta.Load(indexDir)) {
        IE_LOG(WARN, "get meta failed in [%s]", indexDir->DebugString().c_str());
        return false;
    }
    switch (meta.GetType()) {
        case SegmentType::kRaw: {
            mSegment.reset(new RawSegment(meta));
        } break;
        case SegmentType::kPMIndex:
        case SegmentType::kIndex: {
            mSegment.reset(new IndexSegment(meta));
        } break;
        default:
            IE_LOG(ERROR, "unexpected segment type [%d]", int(meta.GetType()));
            return false;
    }
    return mSegment->Load(indexDir, LoadType::kToReduce);
}

}
}
