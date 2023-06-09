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
#include "autil/EnvUtil.h"

namespace autil {

AUTIL_LOG_SETUP(autil, EnvUtil);

std::string EnvUtil::getEnv(const std::string &key, const std::string &defaultValue) {
    const char *str = std::getenv(key.c_str());
    return str == NULL ? defaultValue : std::string(str);
}

bool EnvUtil::setEnv(const std::string& env, const std::string& value, bool overwrite) {
    return setenv(env.c_str(), value.c_str(), overwrite ? 1 : 0) == 0;
}

bool EnvUtil::unsetEnv(const std::string& env)  {
    return unsetenv(env.c_str()) == 0;
}

std::string EnvUtil::envReplace(const std::string &value) {
    size_t first_dollar = value.find('$');
    if (first_dollar == std::string::npos) {
        return value;
    }

    std::string ret = value.substr(0, first_dollar);
    const char *ptr = value.c_str() + first_dollar;
    const char *end = value.c_str() + value.size();
    const char *var_begin = NULL;
    for (; ptr < end; ++ptr) {
        if (*ptr == '$' && *(ptr+1) == '{') {
            if (var_begin != NULL) {
                ret.append(var_begin, ptr - var_begin);
            }
            var_begin = ptr;
            continue;
        } else if(*ptr == '}' && var_begin != NULL) {
            std::string key(var_begin+2, ptr-var_begin-2);
            ret.append(getEnv(key, ""));
            var_begin = NULL;
        } else if(var_begin == NULL) {
            ret.push_back(*ptr);
        }
    }
    if (var_begin != NULL) {
        ret.append(var_begin);
    }
    return ret;
}

}
