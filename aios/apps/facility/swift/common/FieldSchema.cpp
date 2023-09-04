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
#include "swift/common/FieldSchema.h"

#include <exception>
#include <iosfwd>

#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"

using namespace std;
using namespace autil;
namespace swift {
namespace common {
AUTIL_LOG_SETUP(swift, FieldSchema);

bool FieldItem::addSubField(const string &name, const string &type) {
    if (name.empty() || !FieldItem::checkType(type)) {
        return false;
    }
    for (const auto &item : subFields) {
        if (name == item.name) {
            return false;
        }
    }
    FieldItem afield(name, type);
    subFields.emplace_back(afield);
    return true;
}

bool FieldItem::checkType(const std::string &type) { return "string" == type; }

void FieldItem::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("name", name, name);
    if (json.GetMode() == FROM_JSON) {
        json.Jsonize("type", type, type);
        json.Jsonize("subFields", subFields, subFields);
        for (auto const &sub : subFields) {
            if (!FieldItem::checkType(sub.type)) {
                AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException,
                                   string("sub [") + type + "] type not support!");
            }
        }
        if (0 == subFields.size() && !FieldItem::checkType(type)) {
            AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, string("[") + type + "] type not support!");
        }
    } else if (json.GetMode() == TO_JSON) {
        if (0 != subFields.size()) {
            json.Jsonize("subFields", subFields, subFields);
        } else {
            json.Jsonize("type", type, type);
        }
    }
}

void FieldSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("topic", topicName, topicName);
    json.Jsonize("fields", fields, fields);
}

bool FieldSchema::isValid() const {
    if (topicName.empty()) {
        AUTIL_LOG(ERROR, "topic name is empty");
        return false;
    }
    if (0 == fields.size()) {
        AUTIL_LOG(ERROR, "fields is empty");
        return false;
    }
    return true;
}

void FieldSchema::setTopicName(const string &name) { topicName = name; }

bool FieldSchema::addField(const string &name, const string &type) {
    if (name.empty()) {
        AUTIL_LOG(ERROR, "name empty");
        return false;
    }
    if (!FieldItem::checkType(type)) {
        AUTIL_LOG(ERROR, "type [%s] invalid", type.c_str());
        return false;
    }
    for (const auto &item : fields) {
        if (name == item.name) {
            AUTIL_LOG(ERROR, "field[%s] already exist!", name.c_str());
            return false;
        }
    }
    FieldItem item(name, type);
    fields.emplace_back(item);
    return true;
}

bool FieldSchema::addSubField(const string &name, const string &subName, const string &type) {
    if (name.empty() || subName.empty()) {
        AUTIL_LOG(ERROR, "name empty");
        return false;
    }
    for (auto &item : fields) {
        if (name == item.name) {
            return item.addSubField(subName, type);
        }
    }
    AUTIL_LOG(ERROR, "field [%s] not exist", name.c_str());
    return false;
}

bool FieldSchema::addField(const FieldItem &fdItem) {
    for (const auto &item : fields) {
        if (fdItem.name == item.name) {
            AUTIL_LOG(ERROR, "field[%s] already exist!", fdItem.name.c_str());
            return false;
        }
    }
    fields.push_back(fdItem);
    return true;
}

string FieldSchema::toJsonString() const { return ToJsonString(*this, true); }

bool FieldSchema::fromJsonString(const std::string &schemaStr) {
    clear();
    try {
        FromJsonString(*this, schemaStr);
    } catch (std::exception &e) {
        AUTIL_LOG(ERROR, "parse schema error[%s]", e.what());
        return false;
    }
    return true;
}

int32_t FieldSchema::calcVersion() const {
    const string &schemaStr = toJsonString();
    int32_t hash = static_cast<int32_t>(std::hash<string>{}(schemaStr));
    return hash;
}

void FieldSchema::clear() {
    topicName.clear();
    fields.clear();
}

} // namespace common
} // namespace swift
