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

#include "autil/Lock.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegmentBuildResource.h"
#include "indexlib/index/ann/aitheta2/impl/Segment.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"
namespace indexlibv2::index::ann {

class RealtimeSegment : public Segment
{
public:
    RealtimeSegment(const AithetaIndexConfig& indexConfig);
    ~RealtimeSegment();

public:
    bool Open(const SegmentBuildResourcePtr& segmentBuildResource);
    bool DumpIndexData();
    bool DumpSegmentMeta();
    bool EndDump();
    bool Close() override;

public:
    bool GetRealtimeIndex(index_id_t indexId, AiThetaStreamerPtr& index, bool createIfNotExist);

private:
    bool FillSegmentMeta();
    bool InitStreamerMap(const SegmentBuildResourcePtr& segmentResource);

protected:
    mutable autil::ReadWriteLock _streamerMapLock;
    std::unordered_map<index_id_t, AiThetaStreamerPtr> _streamerMap AUTIL_GUARDED_BY(_streamerMapLock);
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RealtimeSegment> RealtimeSegmentPtr;
} // namespace indexlibv2::index::ann
