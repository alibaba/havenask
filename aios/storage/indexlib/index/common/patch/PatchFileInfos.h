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

#include "indexlib/index/common/patch/PatchFileInfo.h"

namespace indexlibv2::index {

class PatchFileInfos : public autil::legacy::Jsonizable
{
public:
    const PatchFileInfo& operator[](size_t idx) const;
    PatchFileInfo& operator[](size_t idx);
    size_t Size() const;
    void PushBack(const PatchFileInfo& patchFileInfo);
    void PushBack(PatchFileInfo&& patchFileInfo);
    void SetPatchFileInfos(const std::vector<PatchFileInfo>& patchFileInfos);
    const std::vector<PatchFileInfo>& GetPatchFileInfos() const;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::string DebugString() const;

public:
    static bool Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, const std::string& fileName,
                     PatchFileInfos* patchFileInfos) noexcept;

private:
    std::vector<PatchFileInfo> _patchFileInfos;

private:
    AUTIL_LOG_DECLARE();
};

using PatchInfos = std::map<segmentid_t, PatchFileInfos>;
using DeletePatchInfos = std::map<segmentid_t, PatchFileInfo>;

} // namespace indexlibv2::index
