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
#ifndef __INDEXLIB_PARAMS_UTIL_H
#define __INDEXLIB_PARAMS_UTIL_H

#include <stdlib.h>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace util {

class ParamsUtil
{
public:
    template <class T>
    static T GetParam(const std::string& key, const std::map<std::string, std::string>& kvMap, const T& defaultValue)
    {
        auto it = kvMap.find(key);
        T ret = T();
        if (it != kvMap.end()) {
            auto success = autil::StringUtil::fromString(it->second, ret);
            if (success) {
                return ret;
            } else {
                return defaultValue;
            }
        }
        return autil::EnvUtil::getEnv(key, defaultValue);
    }

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(ParamsUtil);
}} // namespace indexlib::util

#endif //__INDEXLIB_PARAMS_UTIL_H
