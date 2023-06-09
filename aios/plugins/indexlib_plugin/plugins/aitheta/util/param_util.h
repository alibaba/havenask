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
/**
 * @file   knn_util.h
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Tue Jan 22 10:01:42 2019
 *
 * @brief
 *
 *
 */

#ifndef __INDEXLIB_AITHETA_UTIL_PARAM_UTIL_H_
#define __INDEXLIB_AITHETA_UTIL_PARAM_UTIL_H_

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/misc/log.h"

namespace indexlib {
namespace aitheta_plugin {

class ParamUtil {
 public:
    ParamUtil();
    ~ParamUtil();

 public:
    static void MergeParams(const util::KeyValueMap &srcParams, util::KeyValueMap &params, bool allowCover = false);

    template <typename T>
    static bool ExtractValue(const indexlib::util::KeyValueMap &parameters, const std::string &key, T *val);

    template <typename T, typename... Args>
    static void ExtractValue(const indexlib::util::KeyValueMap &parameters, const std::string &key, T *val,
                             Args... args);

 private:
    IE_LOG_DECLARE();
};

template <typename T>
bool ParamUtil::ExtractValue(const indexlib::util::KeyValueMap &parameters, const std::string &key, T *val) {
    auto iterator = parameters.find(key);
    if (iterator == parameters.end()) {
        return false;
    }

    autil::StringUtil::fromString(iterator->second, *val);

    return true;
}

template <typename T, typename... Args>
void ParamUtil::ExtractValue(const indexlib::util::KeyValueMap &parameters, const std::string &key, T *val,
                             Args... args) {
    ExtractValue(parameters, key, val);
    ExtractValue(parameters, args...);
}

}
}

#endif  // __INDEXLIB_AITHETA_UTIL_PARAM_UTIL_H_
