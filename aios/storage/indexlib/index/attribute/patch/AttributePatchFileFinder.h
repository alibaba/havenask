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
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/patch/SrcDestPatchFileFinder.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::index {

class AttributePatchFileFinder : public SrcDestPatchFileFinder
{
public:
    AttributePatchFileFinder() = default;
    ~AttributePatchFileFinder() = default;

public:
    std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
    GetIndexDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir,
                      const std::shared_ptr<config::IIndexConfig>& indexConfig) const override
    {
        auto attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
        assert(attrConfig);
        std::vector<std::string> indexPaths = attrConfig->GetIndexPath();
        assert(indexPaths.size() == 1);
        std::string indexPath = indexPaths[0];
        if (attrConfig->GetSliceCount() > 1 && attrConfig->GetSliceIdx() >= 0) {
            // slice attribute, patch is in parent path
            indexPath = indexlib::util::PathUtil::GetParentDirPath(indexPath);
        }
        auto [ec, indexDir] = segDir->GetDirectory(indexPath);
        if (ec == indexlib::file_system::ErrorCode::FSEC_NOENT) {
            return {Status::OK(), nullptr};
        }
        if (ec != indexlib::file_system::ErrorCode::FSEC_OK) {
            return {toStatus(ec), nullptr};
        }
        return {Status::OK(), indexDir};
    }
};

} // namespace indexlibv2::index
