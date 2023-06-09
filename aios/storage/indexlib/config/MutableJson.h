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

#include <optional>
#include <type_traits>
#include <typeinfo>

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"

namespace indexlibv2::config {

class MutableJson
{
public:
    MutableJson() = default;
    MutableJson(const MutableJson&) = default;
    virtual ~MutableJson() {};

    // jsonPath examples: key1.key2; key1.key2[1][2]
    template <typename T>
    bool GetValue(const std::string& jsonPath, T* value) const noexcept;
    const autil::legacy::Any* GetAny(const std::string& jsonPath) const noexcept;

    bool SetValue(const std::string& jsonPath, const char* value);
    template <typename T>
    bool SetValue(const std::string& jsonPath, const T& value);
    bool SetAny(const std::string& jsonPath, const autil::legacy::Any& anyValue);

    bool IsEmpty() const { return _json.IsEmpty(); }

public:
    template <typename T>
    static std::pair<bool, T> AdaptiveDecodeJson(const autil::legacy::Any& any);

private:
    struct JsonNodeHint {
        std::string nodeName;
        std::optional<size_t> idxInArray;
        bool isArray = false;
    };
    template <bool readOnly, typename NodeIter, typename AnyType>
    static AnyType* Locate(NodeIter nodeIter, NodeIter nodeIterEnd, AnyType* parent);

    static std::pair<bool, std::vector<JsonNodeHint>> ParseJsonPath(const std::string& jsonPath);
    static bool ContainerTypeMatched(const autil::legacy::Any* any, bool isArray);

protected:
    autil::legacy::Any _json;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <typename T>
inline bool MutableJson::GetValue(const std::string& jsonPath, T* value) const noexcept
{
    const autil::legacy::Any* any = GetAny(jsonPath);
    if (!any) {
        return false;
    }
    try {
        autil::legacy::FromJson(*value, *any);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "decode jsonpath[%s] failed: %s", jsonPath.c_str(), e.what());
        return false;
    }
    return true;
}

template <typename T>
inline bool MutableJson::SetValue(const std::string& jsonPath, const T& value)
{
    try {
        auto any = autil::legacy::ToJson(value);
        return SetAny(jsonPath, any);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "encode jsonpath[%s] failed: %s", jsonPath.c_str(), e.what());
        return false;
    }
}

} // namespace indexlibv2::config
