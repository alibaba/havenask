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
#ifndef HIPPO_JSONUTIL_H
#define HIPPO_JSONUTIL_H

#include "util/Log.h"
#include "common/common.h"
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

BEGIN_HIPPO_NAMESPACE(util);

class JsonUtil
{
public:
    JsonUtil();
    ~JsonUtil();
private:
    JsonUtil(const JsonUtil &);
    JsonUtil& operator=(const JsonUtil &);
public:
    static bool getSubDoc(
            const rapidjson::Document &doc, const std::string &subDocKey,
            rapidjson::Document &subDoc);

    static bool convertToDoc(
            const rapidjson::Value &value, rapidjson::Document &doc);

    static bool fromJson(const std::string &json, rapidjson::Document &doc);
    
    static bool toJson(const rapidjson::Value &value, std::string &json);
    
    static bool toJson(const rapidjson::Document &doc, std::string &json);

    static rapidjson::Document *copyFrom(const rapidjson::Document &doc);
    static void toStrStrMap(const rapidjson::Document &doc, std::map<std::string, std::string> & map);

private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(JsonUtil);

END_HIPPO_NAMESPACE(util);

#endif //HIPPO_JSONUTIL_H
