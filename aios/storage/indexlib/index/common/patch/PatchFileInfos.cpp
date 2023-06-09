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
#include "indexlib/index/common/patch/PatchFileInfos.h"

#include "indexlib/file_system/IDirectory.h"

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, PatchFileInfos);

bool PatchFileInfos::Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                          const std::string& fileName, PatchFileInfos* patchFileInfos) noexcept
{
    auto [st, existRet] = directory->IsExist(fileName).StatusWith();
    if (!st.IsOK() or !existRet) {
        AUTIL_LOG(INFO, "Looking for PatchFileInfo meta file [%s/%s] failed", directory->DebugString().c_str(),
                  fileName.c_str());
        return false;
    }

    std::string content;
    st = directory
             ->Load(fileName, indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM),
                    content)
             .Status();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "Load [%s/%s] failed, status[%s]", directory->DebugString().c_str(), fileName.c_str(),
                  st.ToString().c_str());
        return false;
    }
    patchFileInfos->_patchFileInfos.clear();
    try {
        autil::legacy::FromJsonString(*patchFileInfos, content);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "Load [%s/%s] failed, exception[%s]", directory->DebugString().c_str(), fileName.c_str(),
                  e.what());
        return false;
    } catch (...) {
        AUTIL_LOG(ERROR, "Load [%s/%s] failed", directory->DebugString().c_str(), fileName.c_str());
        return false;
    }
    return true;
}

void PatchFileInfos::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("patch_file_infos", _patchFileInfos, _patchFileInfos);
}

const PatchFileInfo& PatchFileInfos::operator[](size_t idx) const
{
    assert(idx < _patchFileInfos.size());
    return _patchFileInfos[idx];
}
PatchFileInfo& PatchFileInfos::operator[](size_t idx)
{
    assert(idx < _patchFileInfos.size());
    return _patchFileInfos[idx];
}
size_t PatchFileInfos::Size() const { return _patchFileInfos.size(); }
void PatchFileInfos::PushBack(const PatchFileInfo& patchFileInfo) { _patchFileInfos.push_back(patchFileInfo); }
void PatchFileInfos::PushBack(PatchFileInfo&& patchFileInfo) { _patchFileInfos.emplace_back(std::move(patchFileInfo)); }
void PatchFileInfos::SetPatchFileInfos(const std::vector<PatchFileInfo>& patchFileInfos)
{
    _patchFileInfos = patchFileInfos;
}
const std::vector<PatchFileInfo>& PatchFileInfos::GetPatchFileInfos() const { return _patchFileInfos; }

std::string PatchFileInfos::DebugString() const
{
    std::stringstream ss;
    for (const PatchFileInfo& info : _patchFileInfos) {
        ss << info.patchFileName << ", ";
    }
    return ss.str();
}

} // namespace indexlibv2::index
