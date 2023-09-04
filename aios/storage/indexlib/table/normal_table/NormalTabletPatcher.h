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
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/common/patch/PatchFileInfo.h"

namespace indexlibv2::framework {
class Segment;
class TabletData;
} // namespace indexlibv2::framework
namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::table {
class NormalTabletModifier;
class NormalTabletPatcher
{
public:
    NormalTabletPatcher();
    ~NormalTabletPatcher();

public:
    static Status LoadPatch(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                            const framework::TabletData& newTabletData,
                            const std::shared_ptr<config::ITabletSchema>& schema,
                            const std::shared_ptr<indexlib::file_system::IDirectory>& opLog2PatchRootDir,
                            NormalTabletModifier* modifier);
    static size_t CalculatePatchLoadExpandSize(const std::shared_ptr<config::ITabletSchema>& schema,
                                               const std::vector<std::shared_ptr<framework::Segment>>& segments);

private:
    static docid_t GetSegmentBaseDocId(const std::shared_ptr<framework::Segment>& segment,
                                       const framework::TabletData& newTabletData);
    // TODO: @qingran 倒排，相对老架构，这里似乎没有支持并行打Patch
    static Status LoadPatchForDeletionMap(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                          const framework::TabletData& newTabletData,
                                          const std::shared_ptr<config::ITabletSchema>& schema);
    static Status
    LoadPatchForAttribute(const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& segments,
                          const std::shared_ptr<config::ITabletSchema>& schema,
                          const std::shared_ptr<indexlib::file_system::IDirectory>& opLog2PatchRootDir,
                          NormalTabletModifier* modifier);
    static Status
    LoadAttributePatchWithModifier(const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& segments,
                                   const std::shared_ptr<config::ITabletSchema>& schema,
                                   NormalTabletModifier* modifier);
    static Status
    LoadAttributePatch(const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& segments,
                       const std::shared_ptr<config::ITabletSchema>& schema,
                       const std::shared_ptr<indexlib::file_system::IDirectory>& opLog2PatchRootDir);
    static Status LoadPatchForInveredIndex(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                           const std::shared_ptr<config::ITabletSchema>& schema,
                                           const std::shared_ptr<indexlib::file_system::IDirectory>& opLog2PatchRootDir,
                                           NormalTabletModifier* modifier);
    static Status LoadInvertedIndexPatchWithModifier(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                                     const std::shared_ptr<config::ITabletSchema>& schema,
                                                     NormalTabletModifier* modifier);
    static Status LoadInvertedIndexPatch(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                         const std::shared_ptr<config::ITabletSchema>& schema,
                                         const std::shared_ptr<indexlib::file_system::IDirectory>& opLog2PatchRootDir);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
