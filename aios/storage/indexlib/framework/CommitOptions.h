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

#include <map>
#include <optional>

#include "indexlib/base/Constant.h"
#include "indexlib/framework/Locator.h"

namespace indexlibv2::framework {

class CommitOptions
{
public:
    CommitOptions& SetNeedPublish(bool needPublish)
    {
        _needPublish = needPublish;
        return *this;
    }
    CommitOptions& SetTargetVersionId(versionid_t targetVersionId)
    {
        _targetVersionId = targetVersionId;
        return *this;
    }
    CommitOptions& SetNeedReopenInCommit(bool needReopenInCommit)
    {
        _needReopenInCommit = needReopenInCommit;
        return *this;
    }

    bool NeedPublish() const { return _needPublish; }
    bool NeedReopenInCommit() const { return _needReopenInCommit; }
    versionid_t GetTargetVersionId() const { return _targetVersionId; }

    void AddVersionDescription(const std::string& key, const std::string& value) { _versionDesc[key] = value; }
    const std::map<std::string, std::string>& GetVersionDescriptions() const { return _versionDesc; }
    // when build from legacy index or build from diff src
    // tablet first commit version with target src locator
    // to correct right locator info
    void SetSpecificLocator(const Locator& locator) { _specifyLocator = locator; }
    std::optional<Locator> GetSpecificLocator() const { return _specifyLocator; }

private:
    bool _needReopenInCommit = false;
    bool _needPublish = false;
    versionid_t _targetVersionId = INVALID_VERSIONID;
    std::optional<Locator> _specifyLocator;
    std::pair<uint32_t, uint32_t> _progressRange;
    std::map<std::string, std::string> _versionDesc;
};

} // namespace indexlibv2::framework
