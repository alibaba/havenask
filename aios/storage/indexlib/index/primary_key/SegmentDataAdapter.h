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

#include "autil/NoCopyable.h"
#include "indexlib/framework/SegmentInfo.h"

namespace indexlibv2::framework {
class Segment;
}

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::index {

class SegmentDataAdapter : public autil::NoCopyable
{
public:
    SegmentDataAdapter() {}
    ~SegmentDataAdapter() {}

    typedef struct {
        std::shared_ptr<const framework::SegmentInfo> _segmentInfo;
        std::shared_ptr<indexlib::file_system::Directory> _directory;
        segmentid_t _segmentId = INVALID_DOCID;
        bool _isRTSegment = false;
        docid64_t _baseDocId = 0;
        framework::Segment* _segment = nullptr;
    } SegmentDataType;

    static void Transform(const std::vector<framework::Segment*>& segments, std::vector<SegmentDataType>& segmentDatas);
};

} // namespace indexlibv2::index
