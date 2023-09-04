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
#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/IResource.h"
#include "indexlib/table/normal_table/NormalTabletInfo.h"

namespace indexlibv2::framework {
class TabletData;
}

namespace indexlibv2::index {
class DeletionMapIndexReader;
}

namespace indexlibv2::table {
class NormalTabletMeta;

class NormalTabletInfoHolder : public framework::IResource
{
public:
    static std::shared_ptr<NormalTabletInfoHolder>
    CreateOrReinit(const std::shared_ptr<framework::IResource>& lastClonedHolder,
                   const std::shared_ptr<framework::TabletData>& tabletData,
                   const std::shared_ptr<NormalTabletMeta>& tabletMeta,
                   const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader);
    NormalTabletInfoHolder() = default;
    ~NormalTabletInfoHolder() = default;

    std::shared_ptr<framework::IResource> Clone() override;
    size_t CurrentMemmoryUse() const override;

    void UpdateNormalTabletInfo(size_t lastRtSegDocCount);

    std::shared_ptr<NormalTabletInfo> GetNormalTabletInfo() const;

private:
    void Init(const std::shared_ptr<framework::TabletData>& tabletData,
              const std::shared_ptr<NormalTabletMeta>& tabletMeta,
              const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader,
              const NormalTabletInfo::HistorySegmentInfos& hisInfos);
    void SetNormalTabletInfo(const std::shared_ptr<NormalTabletInfo>& normalTabletInfo);

    mutable autil::ReadWriteLock _lock;
    std::shared_ptr<NormalTabletInfo> _normalTabletInfo;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
