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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/SegmentUpdateBitmap.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::file_system {
class IDirectory;
}
namespace indexlibv2::index {

class AttributeUpdateBitmap : private autil::NoCopyable
{
private:
    struct SegmentBaseDocIdInfo {
        SegmentBaseDocIdInfo() : segId(INVALID_SEGMENTID), baseDocId(INVALID_DOCID) {}

        SegmentBaseDocIdInfo(segmentid_t segmentId, docid_t docId) : segId(segmentId), baseDocId(docId) {}

        segmentid_t segId;
        docid_t baseDocId;
    };

    using SegBaseDocIdInfoVect = std::vector<SegmentBaseDocIdInfo>;
    using SegUpdateBitmapVec = std::vector<std::shared_ptr<SegmentUpdateBitmap>>;

public:
    AttributeUpdateBitmap() : _totalDocCount(0) /*, _buildResourceMetricsNode(NULL)*/ {}

    ~AttributeUpdateBitmap() {}

public:
    void Init();

    Status Set(docid_t globalDocId);

    std::shared_ptr<SegmentUpdateBitmap> GetSegmentBitmap(segmentid_t segId)
    {
        for (size_t i = 0; i < _dumpedSegmentBaseIdVec.size(); ++i) {
            if (segId == _dumpedSegmentBaseIdVec[i].segId) {
                return _segUpdateBitmapVec[i];
            }
        }
        return nullptr;
    }

    Status Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& dir) const;

private:
    int32_t GetSegmentIdx(docid_t globalDocId) const
    {
        if (globalDocId >= _totalDocCount) {
            return -1;
        }

        size_t i = 1;
        for (; i < _dumpedSegmentBaseIdVec.size(); ++i) {
            if (globalDocId < _dumpedSegmentBaseIdVec[i].baseDocId) {
                break;
            }
        }
        return int32_t(i) - 1;
    }

    // AttributeUpdateInfo GetUpdateInfoFromBitmap() const;

private:
    indexlib::util::SimplePool _pool;
    SegBaseDocIdInfoVect _dumpedSegmentBaseIdVec;
    SegUpdateBitmapVec _segUpdateBitmapVec;
    docid_t _totalDocCount;
    // util::BuildResourceMetricsNode* _buildResourceMetricsNode;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
