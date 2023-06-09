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
#include "indexlib/table/normal_table/NormalTabletInfoHolder.h"

#include "indexlib/table/normal_table/NormalTabletInfo.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletInfoHolder);

std::shared_ptr<framework::IResource> NormalTabletInfoHolder::Clone()
{
    auto cloneHolder = std::make_shared<NormalTabletInfoHolder>();
    cloneHolder->SetNormalTabletInfo(GetNormalTabletInfo());
    return cloneHolder;
}

size_t NormalTabletInfoHolder::CurrentMemmoryUse() const { return sizeof(NormalTabletInfoHolder); }

Status NormalTabletInfoHolder::Init(const std::shared_ptr<framework::TabletData>& tabletData,
                                    const std::shared_ptr<NormalTabletMeta>& tabletMeta,
                                    const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader)
{
    _normalTabletInfo = std::make_shared<NormalTabletInfo>();
    return _normalTabletInfo->Init(tabletData, tabletMeta, deletionMapReader);
}

void NormalTabletInfoHolder::UpdateNormalTabletInfo(segmentid_t lastRtSegId, size_t lastRtSegDocCount,
                                                    segmentid_t refindRtSegmentId)
{
    if (!_normalTabletInfo->NeedUpdate(lastRtSegId, lastRtSegDocCount, refindRtSegmentId)) {
        return;
    }
    std::shared_ptr<NormalTabletInfo> cloneInfo(_normalTabletInfo->Clone());
    cloneInfo->UpdateDocCount(lastRtSegId, lastRtSegDocCount, refindRtSegmentId);
    SetNormalTabletInfo(cloneInfo);
}

std::shared_ptr<NormalTabletInfo> NormalTabletInfoHolder::GetNormalTabletInfo() const
{
    autil::ScopedReadWriteLock lock(_lock, 'r');
    return _normalTabletInfo;
}

void NormalTabletInfoHolder::SetNormalTabletInfo(const std::shared_ptr<NormalTabletInfo>& normalTabletInfo)
{
    autil::ScopedReadWriteLock lock(_lock, 'w');
    _normalTabletInfo = normalTabletInfo;
}

} // namespace indexlibv2::table
