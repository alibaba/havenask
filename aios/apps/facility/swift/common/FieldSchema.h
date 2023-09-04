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
#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace swift {
namespace common {

struct HeadMeta {
    uint32_t checkval : 20;
    uint8_t dataType  : 3;
    uint16_t reserve  : 9;
};

class SchemaHeader {
public:
    SchemaHeader() : pack(0), version(0) {}
    SchemaHeader(uint32_t v) : pack(0), version(v) {}

public:
    void toBufHead(char *buf) {
        uint8_t *p = (uint8_t *)buf;
        for (int i = 0; i < 4; ++i) {
            p[i] = (pack >> (8 * i)) & 255;
        }
    }

    static uint32_t getCheckval(uint32_t check) { return check & 0x000FFFFF; }

    void toBufVersion(char *buf) {
        uint8_t *p = (uint8_t *)buf;
        for (int i = 0; i < 4; ++i) {
            p[i + 4] = (version >> (8 * i)) & 255;
        }
    }

    void fromBufHead(const char *buf) {
        pack = 0;
        const uint8_t *p = (const uint8_t *)buf;
        for (int i = 0; i < 4; ++i) {
            pack |= uint32_t(p[i]) << (8 * i);
        }
    }

    void fromBufVersion(const char *buf) {
        version = 0;
        const uint8_t *p = (const uint8_t *)buf;
        for (int i = 0; i < 4; ++i) {
            version |= uint32_t(p[i + 4]) << (8 * i);
        }
    }

    void toBuf(char *buf) {
        toBufHead(buf);
        toBufVersion(buf);
    }

    void fromBuf(const char *buf) {
        fromBufHead(buf);
        fromBufVersion(buf);
    }

    bool operator==(const SchemaHeader &header) { return pack == header.pack && version == header.version; }

public:
    union {
        struct HeadMeta meta;
        uint32_t pack;
    };
    int32_t version;
};

class FieldItem : public autil::legacy::Jsonizable {
public:
    FieldItem() {}
    FieldItem(const std::string &name_, const std::string &type_ = "string") : name(name_), type(type_) {}
    virtual ~FieldItem() {}

public:
    bool addSubField(const std::string &name, const std::string &type = "string");
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    static bool checkType(const std::string &type);

public:
    std::string name;
    std::string type;
    std::vector<FieldItem> subFields;
};

class FieldSchema : public autil::legacy::Jsonizable {
public:
    FieldSchema(){};
    virtual ~FieldSchema(){};

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool isValid() const;
    void setTopicName(const std::string &name);
    bool addField(const std::string &name, const std::string &type = "string");
    bool addSubField(const std::string &name, const std::string &subName, const std::string &type = "string");
    bool addField(const FieldItem &fdItem);
    std::string toJsonString() const;
    bool fromJsonString(const std::string &schemaStr);
    int32_t calcVersion() const;
    void clear();

public:
    std::string topicName;
    std::vector<FieldItem> fields;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace swift
