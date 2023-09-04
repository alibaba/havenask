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
#ifndef CARBON_JSONUTIL_H
#define CARBON_JSONUTIL_H

#include "common/common.h"
#include "carbon/Log.h"
#include "autil/legacy/jsonizable.h"

BEGIN_CARBON_NAMESPACE(common);

class JsonUtil
{
public:
    JsonUtil();
    ~JsonUtil();
private:
    JsonUtil(const JsonUtil &);
    JsonUtil& operator=(const JsonUtil &);
public:
    static bool create(autil::legacy::json::JsonMap *json,
                       const std::string &path,
                       const autil::legacy::Any &value,
                       ErrorInfo *errorInfo);

    static bool update(autil::legacy::json::JsonMap *json,
                       const std::string &path,
                       const autil::legacy::Any &value,
                       ErrorInfo *errorInfo);

    static bool read(const autil::legacy::json::JsonMap &json,
                     const std::string &path,
                     autil::legacy::Any *value,
                     ErrorInfo *errorInfo);
    
    static bool del(autil::legacy::json::JsonMap *json,
                    const std::string &path,
                    ErrorInfo *errorInfo);
    template<typename T>
    static std::string serializeToString(const T&t);
private:
    CARBON_LOG_DECLARE();
};

template<typename T>
std::string JsonUtil::serializeToString(const T&t) {
    try {
        return ToJsonString(t, true);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "jsonize failed, error:[%s].", e.what());
        return std::string("");
    }
}

CARBON_TYPEDEF_PTR(JsonUtil);

END_CARBON_NAMESPACE(common);

#endif //CARBON_JSONUTIL_H
