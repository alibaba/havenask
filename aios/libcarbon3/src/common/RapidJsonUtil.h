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

#ifndef CARBON_RAPIDJSONUTIL_H
#define CARBON_RAPIDJSONUTIL_H

#include "carbon/Log.h"
#include "common/common.h"
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

BEGIN_CARBON_NAMESPACE(common);

class RapidJsonUtil
{
public:
    RapidJsonUtil();
    ~RapidJsonUtil();
private:
    RapidJsonUtil(const RapidJsonUtil &);
    RapidJsonUtil& operator=(const RapidJsonUtil &);
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

private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(RapidJsonUtil);

END_CARBON_NAMESPACE(common);

#endif //CARBON_RAPIDJSONUTIL_H
