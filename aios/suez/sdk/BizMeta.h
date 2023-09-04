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
#include <stddef.h>
#include <string>

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "suez/sdk/JsonNodeRef.h"
#include "suez/sdk/SuezError.h"

namespace suez {

class BizMeta final : public autil::legacy::Jsonizable {
public:
    BizMeta() : keepCount(BIZ_KEEP_COUNT) {}

    BizMeta(const std::string &remoteConfigPath_) : remoteConfigPath(remoteConfigPath_), keepCount(BIZ_KEEP_COUNT) {}

    BizMeta(const std::string &localConfigPath_, const std::string &remoteConfigPath_)
        : localConfigPath(localConfigPath_), remoteConfigPath(remoteConfigPath_), keepCount(BIZ_KEEP_COUNT) {}

    BizMeta(const BizMeta &other) { *this = other; }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("config_path", remoteConfigPath, remoteConfigPath);
        json.Jsonize("keep_count", keepCount, keepCount);
        json.Jsonize("custom_biz_info", customBizInfo, customBizInfo);
        json.Jsonize("used_tables", usedTables, usedTables);
        if (json.GetMode() == TO_JSON && INVALID_ERROR_CODE != error.errCode) {
            json.Jsonize("error_info", error, error);
        }
    }

    const std::string &getRemoteConfigPath() const { return remoteConfigPath; }

    void setRemoteConfigPath(const std::string &remoteConfigPath_) { remoteConfigPath = remoteConfigPath_; }

    void setError(const SuezError &error_) { error = error_; }

    const SuezError &getError() const { return error; }

    const std::string &getLocalConfigPath() const { return localConfigPath; }

    void setLocalConfigPath(const std::string &localConfigPath_) { localConfigPath = localConfigPath_; }

    BizMeta &operator=(const BizMeta &other) {
        if (this == &other) {
            return *this;
        }
        localConfigPath = other.localConfigPath;
        remoteConfigPath = other.remoteConfigPath;
        keepCount = other.keepCount;
        error = other.error;
        customBizInfo = JsonNodeRef::copy(other.customBizInfo);
        usedTables = other.usedTables;
        return *this;
    }

    bool operator==(const BizMeta &other) const {
        return remoteConfigPath == other.remoteConfigPath && usedTables == other.usedTables &&
               (autil::legacy::FastToJsonString(customBizInfo) == autil::legacy::FastToJsonString(other.customBizInfo));
    }

    bool operator!=(const BizMeta &other) const { return !(*this == other); }

    size_t getKeepCount() const { return keepCount; }

    void setKeepCount(size_t keepCount_) { keepCount = keepCount_; }

    const std::vector<std::string> &getUsedTables() const { return usedTables; }

    bool needDeploy() const { return !remoteConfigPath.empty(); }

    const autil::legacy::json::JsonMap &getCustomBizInfo() const { return customBizInfo; }

    void setCustomBizInfo(const autil::legacy::json::JsonMap &customBizInfo_) { customBizInfo = customBizInfo_; }

private:
    std::string localConfigPath;
    std::string remoteConfigPath;
    size_t keepCount;
    SuezError error;
    autil::legacy::json::JsonMap customBizInfo;
    std::vector<std::string> usedTables;
    static constexpr size_t BIZ_KEEP_COUNT = 5;
};

using BizMetas = std::map<std::string, BizMeta>;
using AppMeta = BizMeta;

} // namespace suez
