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
#include "util/JsonUtil.h"
#include <sstream>
#include <rapidjson/error/en.h>

using namespace std;
using namespace rapidjson;

BEGIN_HIPPO_NAMESPACE(util);

HIPPO_LOG_SETUP(util, JsonUtil);

JsonUtil::JsonUtil() {
}

JsonUtil::~JsonUtil() {
}

bool JsonUtil::getSubDoc(
        const rapidjson::Document &doc, const std::string &subDocKey,
        rapidjson::Document &subDoc)
{
    if (!doc.HasMember(subDocKey.c_str())) {
        HIPPO_LOG(ERROR, "sub doc for key [%s] not exist.", subDocKey.c_str());
        return false;
    }
    return convertToDoc(doc[subDocKey.c_str()], subDoc);
}

bool JsonUtil::convertToDoc(
        const rapidjson::Value &value, rapidjson::Document &doc)
{
    string json;
    if (!toJson(value, json)) {
        return false;
    }
    
    return fromJson(json, doc);
}

bool JsonUtil::fromJson(const std::string &json, rapidjson::Document &doc) {
    doc.Parse(json.c_str());
    if (doc.HasParseError()) {
        std::stringstream msg;
        msg <<"error at pos:"<<doc.GetErrorOffset()<<", "<<rapidjson::GetParseError_En(doc.GetParseError());
        HIPPO_LOG(ERROR, "invalid json err:%s, str:%s", msg.str().c_str(), json.c_str());        
        return false;
    }
    return true;
}

bool JsonUtil::toJson(const rapidjson::Value &value, std::string &json) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    if (!value.Accept(writer)) {
        return false;
    }
    json = buffer.GetString();
    return true;
}
    
bool JsonUtil::toJson(const rapidjson::Document &doc, std::string &json) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    if (!doc.Accept(writer)) {
        return false;
    }
    json = buffer.GetString();
    return true;
}

void JsonUtil::toStrStrMap(const rapidjson::Document &doc, std::map<std::string, std::string> & map) {
    for(rapidjson::Value::ConstMemberIterator iter = doc.MemberBegin();iter != doc.MemberEnd();iter++) {
        const char * key = iter->name.GetString();
        const rapidjson::Value& val = iter->value;
        if(val.IsString()) {
            map.insert(make_pair(key, val.GetString()));
        }
    }
}

rapidjson::Document* JsonUtil::copyFrom(const rapidjson::Document &doc) {
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

END_HIPPO_NAMESPACE(util);

