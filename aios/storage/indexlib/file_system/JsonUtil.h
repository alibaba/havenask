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

#include <assert.h>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

namespace indexlib { namespace file_system {

// Don't include JsonUtil in .h files.
// JsonUtil provides interfaces to Json objects to Load and Store to physical storage.
class JsonUtil
{
public:
    template <typename Json>
    static FSResult<void> FromString(const std::string& content, Json* json);
    template <typename Json>
    static FSResult<void> ToString(const Json& json, std::string* content);

    template <typename Json>
    static FSResult<Json> FromString(const std::string& content);
    template <typename Json>
    static FSResult<std::string> ToString(const Json& json);

    template <typename Json>
    static FSResult<int64_t> Load(const std::string& path, Json* json);
    template <typename Json>
    static FSResult<void> Store(const Json& json, const std::string& path, bool overwrite = false);

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <typename Json>
FSResult<void> JsonUtil::FromString(const std::string& content, Json* json)
{
    try {
        autil::legacy::FromJsonString(*json, content);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "From json string failed, exception[%s]", e.what());
        return FSEC_ERROR;
    } catch (...) {
        AUTIL_LOG(ERROR, "From json string failed");
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

template <typename Json>
FSResult<void> JsonUtil::ToString(const Json& json, std::string* content)
{
    assert(content);
    try {
        *content = autil::legacy::ToJsonString(json);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "To json string failed, exception[%s]", e.what());
        return FSEC_ERROR;
    } catch (...) {
        AUTIL_LOG(ERROR, "To json string failed");
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

template <typename Json>
FSResult<Json> JsonUtil::FromString(const std::string& content)
{
    Json json;
    auto ec = FromString(content, &json);
    return {ec, json};
}

template <typename Json>
FSResult<std::string> JsonUtil::ToString(const Json& json)
{
    std::string content;
    auto ec = ToString(json, &content).Code();
    return {ec, content};
}

template <typename Json>
FSResult<int64_t> JsonUtil::Load(const std::string& path, Json* json)
{
    assert(json);
    std::string content;
    ErrorCode ec = FslibWrapper::AtomicLoad(path, content).Code();
    if (ec == FSEC_NOENT) {
        AUTIL_LOG(INFO, "Load json string failed, path[%s] does not exist", path.c_str());
        return {ec, -1};
    } else if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "Load json string failed, path[%s], ec[%d]", path.c_str(), ec);
        return {ec, -1};
    }

    try {
        autil::legacy::FromJsonString(*json, content);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "From json string failed, path[%s], exception[%s]", path.c_str(), e.what());
        return {FSEC_ERROR, -1};
    } catch (...) {
        AUTIL_LOG(ERROR, "From json string failed, path[%s]", path.c_str());
        return {FSEC_ERROR, -1};
    }
    return {FSEC_OK, static_cast<int64_t>(content.size())};
}

template <typename Json>
FSResult<void> JsonUtil::Store(const Json& json, const std::string& path, bool overwrite)
{
    std::string content;
    try {
        content = autil::legacy::ToJsonString(json);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "To json string failed, path[%s], exception[%s]", path.c_str(), e.what());
        return FSEC_ERROR;
    } catch (...) {
        AUTIL_LOG(ERROR, "To json string failed, path[%s]", path.c_str());
        return FSEC_ERROR;
    }
    ErrorCode ec = FslibWrapper::AtomicStore(path, content, /*removeIfExist=*/overwrite);
    if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(ERROR, "Atomic store failed path[%s]", path.c_str());
        return ec;
    }
    return FSEC_OK;
}
}} // namespace indexlib::file_system
