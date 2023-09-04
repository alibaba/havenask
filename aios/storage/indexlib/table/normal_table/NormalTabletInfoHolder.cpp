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
    auto normalTabletInfo = GetNormalTabletInfo();
    std::shared_ptr<NormalTabletInfo> cloneInfo(normalTabletInfo->Clone());
    cloneHolder->SetNormalTabletInfo(cloneInfo);
    return cloneHolder;
}

size_t NormalTabletInfoHolder::CurrentMemmoryUse() const { return sizeof(NormalTabletInfoHolder); }

void NormalTabletInfoHolder::Init(const std::shared_ptr<framework::TabletData>& tabletData,
                                  const std::shared_ptr<NormalTabletMeta>& tabletMeta,
                                  const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader,
                                  const NormalTabletInfo::HistorySegmentInfos& hisInfos)
{
    _normalTabletInfo = std::make_shared<NormalTabletInfo>();
    _normalTabletInfo->Init(tabletData, tabletMeta, deletionMapReader, hisInfos);
}

void NormalTabletInfoHolder::UpdateNormalTabletInfo(size_t lastRtSegDocCount)
{
    if (!_normalTabletInfo->NeedUpdate(lastRtSegDocCount)) {
        return;
    }
    std::shared_ptr<NormalTabletInfo> cloneInfo(_normalTabletInfo->Clone());
    cloneInfo->UpdateDocCount(lastRtSegDocCount);
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

std::shared_ptr<NormalTabletInfoHolder>
NormalTabletInfoHolder::CreateOrReinit(const std::shared_ptr<framework::IResource>& lastClonedHolder,
                                       const std::shared_ptr<framework::TabletData>& tabletData,
                                       const std::shared_ptr<NormalTabletMeta>& tabletMeta,
                                       const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader)
{
    NormalTabletInfo::HistorySegmentInfos historySegmentInfos;
    std::shared_ptr<NormalTabletInfoHolder> result;
    auto lastHolder = std::dynamic_pointer_cast<NormalTabletInfoHolder>(lastClonedHolder);
    if (lastHolder && lastHolder->GetNormalTabletInfo()) {
        result = lastHolder;
        historySegmentInfos = lastHolder->GetNormalTabletInfo()->GetSegmentSortInfoHis();
    } else {
        result = std::make_shared<NormalTabletInfoHolder>();
    }
    result->Init(tabletData, tabletMeta, deletionMapReader, historySegmentInfos);
    return result;
}

} // namespace indexlibv2::table
