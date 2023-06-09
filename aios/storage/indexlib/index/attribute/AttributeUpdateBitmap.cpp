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
#include "indexlib/index/attribute/AttributeUpdateBitmap.h"

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeUpdateBitmap);

void AttributeUpdateBitmap::Init()
{
    // un-support operation
    assert(false);
}

Status AttributeUpdateBitmap::Set(docid_t globalDocId)
{
    int32_t idx = GetSegmentIdx(globalDocId);
    if (idx == -1) {
        return Status::OK();
    }
    const SegmentBaseDocIdInfo& segBaseIdInfo = _dumpedSegmentBaseIdVec[idx];
    docid_t localDocId = globalDocId - segBaseIdInfo.baseDocId;
    std::shared_ptr<SegmentUpdateBitmap> segUpdateBitmap = _segUpdateBitmapVec[idx];
    if (!segUpdateBitmap) {
        AUTIL_LOG(ERROR, "fail to set update info for doc:%d", globalDocId);
        return Status::InternalError("fail to set update info for doc:%d", globalDocId);
    }
    return segUpdateBitmap->Set(localDocId);
}

// AttributeUpdateInfo AttributeUpdateBitmap::GetUpdateInfoFromBitmap() const
// {
//     AttributeUpdateInfo updateInfo;
//     for (size_t i = 0; i < _segUpdateBitmapVec.size(); ++i) {
//         const SegmentUpdateBitmapPtr& segUpdateBitmap = _segUpdateBitmapVec[i];
//         if (segUpdateBitmap) {
//             size_t updateDocCount = segUpdateBitmap->GetUpdateDocCount();
//             if (updateDocCount > 0) {
//                 updateInfo.Add(SegmentUpdateInfo(_dumpedSegmentBaseIdVec[i].segId, updateDocCount));
//             }
//         }
//     }
//     return updateInfo;
// }

Status AttributeUpdateBitmap::Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& dir) const
{
    AUTIL_LOG(ERROR, "un-implement operation");
    return Status::InternalError("un-implement operation");
    // assert(dir);
    // AttributeUpdateInfo updateInfo = GetUpdateInfoFromBitmap();
    // if (updateInfo.Size() > 0) {
    //     string jsonStr = autil::legacy::ToJsonString(updateInfo);
    //     dir->Store(ATTRIBUTE_UPDATE_INFO_FILE_NAME, jsonStr, WriterOption::AtomicDump());
    // }
}
} // namespace indexlibv2::index
