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

#include <stdlib.h>
#include <string>

#include "autil/Log.h"
#include "autil/StringUtil.h"

namespace autil {

class EnvUtil {
public:
    template <typename T>
    static T getEnv(const std::string& key, const T& defaultValue) {
        const char* str = std::getenv(key.c_str());
        if (!str) {
            return defaultValue;
        }
        T ret = T();
        auto success = StringUtil::fromString(str, ret);
        if (success) {
            return ret;
        } else {
            AUTIL_LOG(WARN, "failed to parse env [%s:%s] to typed, use default value [%s]",
                      key.c_str(), str, StringUtil::toString(defaultValue).c_str());
            return defaultValue;
        }
    }
    template <typename T>
    static bool getEnvWithoutDefault(const std::string& key, T &value) {
        const char* str = std::getenv(key.c_str());
        if (!str) {
            return false;
        }
        return StringUtil::fromString(str, value);
    }

    static std::string getEnv(const std::string& key, const std::string& defaulValue = "");
    
    static bool setEnv(const std::string& env, const std::string& value, bool overwrite = true);

    static bool unsetEnv(const std::string& env);
    
    static std::string envReplace(const std::string &value);
private:
    AUTIL_LOG_DECLARE();
};

class EnvGuard
{
public:
    EnvGuard(const std::string& key, const std::string& newValue) : mKey(key)
    {
        mOldValue = std::getenv(key.c_str());
        EnvUtil::setEnv(key, newValue);
    }
    ~EnvGuard()
    {
        if (!mOldValue) {
            EnvUtil::unsetEnv(mKey);
        } else {
            EnvUtil::setEnv(mKey, std::string(mOldValue));
        }
    }

private:
    std::string mKey;
    char* mOldValue;
};


}

