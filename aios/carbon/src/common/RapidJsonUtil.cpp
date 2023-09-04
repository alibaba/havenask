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
#include "common/RapidJsonUtil.h"

using namespace std;
using namespace rapidjson;
BEGIN_CARBON_NAMESPACE(common);

CARBON_LOG_SETUP(common, RapidJsonUtil);

RapidJsonUtil::RapidJsonUtil() {
}

RapidJsonUtil::~RapidJsonUtil() {
}

bool RapidJsonUtil::getSubDoc(
        const rapidjson::Document &doc, const std::string &subDocKey,
        rapidjson::Document &subDoc)
{
    if (!doc.HasMember(subDocKey.c_str())) {
        CARBON_LOG(ERROR, "sub doc for key [%s] not exist.", subDocKey.c_str());
        return false;
    }
    return convertToDoc(doc[subDocKey.c_str()], subDoc);
}

bool RapidJsonUtil::convertToDoc(
        const rapidjson::Value &value, rapidjson::Document &doc)
{
    string json;
    if (!toJson(value, json)) {
        return false;
    }
    
    return fromJson(json, doc);
}

bool RapidJsonUtil::fromJson(const std::string &json, rapidjson::Document &doc) {
    doc.Parse(json.c_str());
    if (doc.HasParseError()) {
        CARBON_LOG(ERROR, "invalid json str:%s", json.c_str());        
        return false;
    }
    return true;
}

bool RapidJsonUtil::toJson(const rapidjson::Value &value, std::string &json) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    if (!value.Accept(writer)) {
        return false;
    }
    json = buffer.GetString();
    return true;
}
    
bool RapidJsonUtil::toJson(const rapidjson::Document &doc, std::string &json) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    if (!doc.Accept(writer)) {
        return false;
    }
    json = buffer.GetString();
    return true;
}

rapidjson::Document* RapidJsonUtil::copyFrom(const rapidjson::Document &doc) {
    rapidjson::Document *newDoc = new rapidjson::Document;
    string jsonStr;
    if (!toJson(doc, jsonStr)) {
        delete newDoc;
        return NULL;
    }
    if (!fromJson(jsonStr, *newDoc)) {
        delete newDoc;
        return NULL;
    }
    return newDoc;
}

END_CARBON_NAMESPACE(common);

