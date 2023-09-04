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

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {

//通信协议，不要随便修改,否则要考虑通信兼容问题
struct MergeResult : public autil::legacy::Jsonizable {
    versionid_t baseVersionId = INVALID_VERSIONID;
    versionid_t targetVersionId = INVALID_VERSIONID;

    void Jsonize(JsonWrapper& json) override
    {
        json.Jsonize("base_version_id", baseVersionId, baseVersionId);
        json.Jsonize("target_version_id", targetVersionId, targetVersionId);
    }
    MergeResult() = default;
};

//内部状态，可以修改
struct MergeTaskStatus {
    enum Code {
        DONE,
        ERROR,
        UNKNOWN,
    };
    Version baseVersion;
    Version targetVersion;
    Code code = UNKNOWN;

    MergeTaskStatus() = default;
};

enum class Action { ADD = 0, SUSPEND = 1, ABORT = 2, OVERWRITE = 3, UNKNOWN = 128 };

class ActionConvertUtil
{
public:
    static std::string ActionToStr(Action action)
    {
        switch (action) {
        case Action::ADD:
            return "add";
        case Action::SUSPEND:
            return "suspend";
        case Action::ABORT:
            return "abort";
        case Action::OVERWRITE:
            return "overwrite";
        case Action::UNKNOWN:
            return "unknown";
        default:
            return "unknown";
        }
    }
    static Action StrToAction(const std::string& actionStr)
    {
        if (actionStr == "add") {
            return Action::ADD;
        } else if (actionStr == "suspend") {
            return Action::SUSPEND;
        } else if (actionStr == "abort") {
            return Action::ABORT;
        } else if (actionStr == "overwrite") {
            return Action::OVERWRITE;
        } else {
            return Action::UNKNOWN;
        }
    }
};

} // namespace indexlibv2::framework
