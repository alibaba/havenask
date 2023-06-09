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
#include "indexlib/file_system/IndexFileList.h"

#include <algorithm> // std::set_difference, std::sort, std::remove_if
#include <cstddef>
#include <iterator>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, IndexFileList);

void IndexFileList::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) noexcept(false)
{
    json.Jsonize("deploy_file_metas", deployFileMetas, deployFileMetas);
    json.Jsonize("lifecycle", lifecycle, lifecycle);
    if (json.GetMode() != TO_JSON || !finalDeployFileMetas.empty()) {
        json.Jsonize("final_deploy_file_metas", finalDeployFileMetas, finalDeployFileMetas);
    }
}

void IndexFileList::FromString(const string& indexMetaStr) noexcept(false)
{
    deployFileMetas.clear();
    if (indexMetaStr.size() > 0 && indexMetaStr[0] == '{') {
        autil::legacy::FromJsonString(*this, indexMetaStr);
    } else {
        // for compatible for old deploy_index
        vector<string> fileList = StringUtil::split(indexMetaStr, "\n");
        deployFileMetas.reserve(fileList.size());
        for (size_t i = 0; i < fileList.size(); ++i) {
            deployFileMetas.push_back(FileInfo(fileList[i]));
        }
    }
}

string IndexFileList::ToString() const noexcept(false) { return autil::legacy::ToJsonString(*this); }

void IndexFileList::Append(const FileInfo& fileMeta) noexcept { deployFileMetas.push_back(fileMeta); }

void IndexFileList::AppendFinal(const FileInfo& fileMeta) noexcept { finalDeployFileMetas.push_back(fileMeta); }

FSResult<int64_t> IndexFileList::Load(const std::string& path) noexcept
{
    // return JsonUtil::Load(path, this);

    // for compatible for old deploy_index, such as suez ut
    std::string content;
    ErrorCode ec = FslibWrapper::AtomicLoad(path, content).Code();
    if (ec == FSEC_NOENT) {
        AUTIL_LOG(INFO, "Load json string failed, path[%s] does not exist", path.c_str());
        return {ec, -1};
    } else if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "Load json string failed, path[%s], ec[%d]", path.c_str(), ec);
        return {ec, -1};
    }

    if (content.size() > 0 && content[0] == '{') {
        return {JsonUtil::FromString(content, this).Code(), static_cast<int64_t>(content.size())};
    }
    vector<string> fileList = StringUtil::split(content, "\n");
    for (size_t i = 0; i < fileList.size(); ++i) {
        deployFileMetas.push_back(FileInfo(fileList[i]));
    }
    return {FSEC_OK, (int64_t)content.size()};
}

bool IndexFileList::Load(const std::shared_ptr<IDirectory>& directory, const string& fileName, bool* exist) noexcept
{
    auto [ec, existRet] = directory->IsExist(fileName);
    if (ec != FSEC_OK) {
        AUTIL_LOG(INFO, "IsExist [%s/%s] failed", directory->DebugString().c_str(), fileName.c_str());
        return false;
    }
    if (exist) {
        *exist = existRet;
    }
    if (!existRet) {
        return false;
    }

    string content;
    ec = directory->Load(fileName, ReaderOption::PutIntoCache(FSOT_MEM), content).Code();
    if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(INFO, "Load [%s/%s] failed, ec[%d]", directory->DebugString().c_str(), fileName.c_str(), ec);
        return false;
    }
    try {
        FromString(content);
    } catch (const std::exception& e) {
        AUTIL_LOG(INFO, "Load [%s/%s] failed, exception[%s]", directory->DebugString().c_str(), fileName.c_str(),
                  e.what());
        return false;
    } catch (...) {
        AUTIL_LOG(INFO, "Load [%s/%s] failed", directory->DebugString().c_str(), fileName.c_str());
        return false;
    }

    return true;
}

void IndexFileList::Sort() noexcept
{
    std::sort(deployFileMetas.begin(), deployFileMetas.end(), FileInfo::PathCompare);
    std::sort(finalDeployFileMetas.begin(), finalDeployFileMetas.end(), FileInfo::PathCompare);
}

void IndexFileList::Dedup() noexcept
{
    std::sort(deployFileMetas.begin(), deployFileMetas.end(), FileInfo::PathCompare);
    auto iter1 = std::unique(deployFileMetas.begin(), deployFileMetas.end(), FileInfo::IsPathEqual);
    deployFileMetas.erase(iter1, deployFileMetas.end());

    std::sort(finalDeployFileMetas.begin(), finalDeployFileMetas.end(), FileInfo::PathCompare);
    auto iter2 = std::unique(finalDeployFileMetas.begin(), finalDeployFileMetas.end(), FileInfo::IsPathEqual);
    finalDeployFileMetas.erase(iter2, finalDeployFileMetas.end());
}

void IndexFileList::FromDifference(IndexFileList& lhs, IndexFileList& rhs) noexcept
{
    lhs.Sort();
    rhs.Sort();

    deployFileMetas.clear();
    set_difference(lhs.deployFileMetas.begin(), lhs.deployFileMetas.end(), rhs.deployFileMetas.begin(),
                   rhs.deployFileMetas.end(), inserter(deployFileMetas, deployFileMetas.begin()),
                   FileInfo::PathCompare);

    finalDeployFileMetas.clear();
    set_difference(lhs.finalDeployFileMetas.begin(), lhs.finalDeployFileMetas.end(), rhs.finalDeployFileMetas.begin(),
                   rhs.finalDeployFileMetas.end(), inserter(finalDeployFileMetas, finalDeployFileMetas.begin()),
                   FileInfo::PathCompare);
}

void IndexFileList::Filter(function<bool(const FileInfo&)> predicate) noexcept
{
    deployFileMetas.erase(remove_if(deployFileMetas.begin(), deployFileMetas.end(), predicate), deployFileMetas.end());
    finalDeployFileMetas.erase(remove_if(finalDeployFileMetas.begin(), finalDeployFileMetas.end(), predicate),
                               finalDeployFileMetas.end());
}
}} // namespace indexlib::file_system
