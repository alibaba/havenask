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
#ifndef FSLIB_ENVUTIL_H
#define FSLIB_ENVUTIL_H

#include "fslib/common/common_define.h"
#include "autil/StringUtil.h"
#include <stdlib.h>
#include <string>

FSLIB_BEGIN_NAMESPACE(util);

class EnvUtil
{
public:
    EnvUtil() = delete;

public:
    template<typename T>
    static T GetEnv(const std::string& key, const T &defaulValue)
    {
        const char *str = getenv(key.c_str());
        if (!str)
        {
            return defaulValue;
        }
        T ret = T();
        auto success = autil::StringUtil::fromString(str, ret);
        if (success)
        {
            return ret;
        }
        else
        {
            return defaulValue;
        }
    }

    static int SetEnv(const std::string& env, const std::string& value)
    {
        return setenv(env.c_str(), value.c_str(), 1);
    }

    static int UnsetEnv(const std::string& env)
    {
        return unsetenv(env.c_str());
    }
};


FSLIB_END_NAMESPACE(util);

#endif
