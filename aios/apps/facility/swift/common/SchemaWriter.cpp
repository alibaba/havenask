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
#include "swift/common/SchemaWriter.h"

#include <cstddef>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
namespace swift {
namespace common {
AUTIL_LOG_SETUP(swift, SchemaWriter);

SchemaWriter::SchemaWriter() : _version(0) {}

SchemaWriter::~SchemaWriter() {
    for (auto &field : _fieldMap) {
        for (auto &sub : field.second.subFields) {
            DELETE_AND_SET_NULL(sub.second);
        }
    }
}

bool SchemaWriter::loadSchema(const std::string &schemaStr) {
    _schema.clear();
    if (!_schema.fromJsonString(schemaStr)) {
        AUTIL_LOG(ERROR, "load schema error[%s]", schemaStr.c_str());
        return false;
    }
    if (_schema.topicName.empty()) {
        AUTIL_LOG(ERROR, "parse schema error, topic not set");
        return false;
    }
    if (_schema.fields.empty()) {
        AUTIL_LOG(ERROR, "parse schema error, fields not set");
        return false;
    }
    generateFieldMap();
    _version = _schema.calcVersion();
    return true;
}

void SchemaWriter::generateFieldMap() {
    _fieldMap.clear();
    for (const auto &field : _schema.fields) {
        SchemaWriterField sfield;
        for (const auto &subField : field.subFields) {
            sfield.subFields.insert(make_pair(subField.name, new SchemaWriterField()));
        }
        _fieldMap.insert(make_pair(field.name, sfield));
    }
}

void SchemaWriter::reset() {
    for (auto &field : _fieldMap) {
        field.second.isNone = true;
        for (auto &sub : field.second.subFields) {
            sub.second->isNone = true;
        }
    }
}

void SchemaWriter::setVersion(int32_t version) { _version = version; }

int32_t SchemaWriter::getVersion() { return _version; }

string SchemaWriter::getSchemaStr() const { return _schema.toJsonString(); }

bool SchemaWriter::setField(const string &name, const string &value, bool isNone) {
    auto iter = _fieldMap.find(name);
    if (iter != _fieldMap.end()) {
        iter->second.isNone = isNone;
        iter->second.value = value;
        return true;
    } else {
        return false;
    }
}

bool SchemaWriter::setSubField(const string &name, const string &subName, const string &value, bool isNone) {
    auto iter = _fieldMap.find(name);
    if (iter != _fieldMap.end()) {
        auto subIter = iter->second.subFields.find(subName);
        if (subIter != iter->second.subFields.end()) {
            subIter->second->isNone = isNone;
            subIter->second->value = value;
            return true;
        }
    }
    return false;
}

string SchemaWriter::toString() {
    string result;
    writeHeader(result);
    for (const auto &field : _schema.fields) {
        const SchemaWriterField &mfield = _fieldMap[field.name];
        writeBool(result, mfield.isNone);
        if (!mfield.isNone) {
            uint32_t len = (uint32_t)mfield.value.size();
            writeLength(result, len);
            if (len > 0) {
                writeBytes(result, mfield.value);
            }
        }
        for (const auto &sub : field.subFields) {
            const auto iter = mfield.subFields.find(sub.name);
            writeBool(result, iter->second->isNone);
            if (!iter->second->isNone) {
                uint32_t subLen = (uint32_t)iter->second->value.size();
                writeLength(result, subLen);
                if (subLen > 0) {
                    writeBytes(result, iter->second->value);
                }
            }
        }
    }
    return result;
}

string SchemaWriter::toStringWithHeader() {
    string result = toString();
    result.insert(0, sizeof(HeadMeta), 0);
    return result;
}

void SchemaWriter::writeLength(string &dataStr, uint32_t value) {
    size_t pos = 0;
    (unsigned char &)_varintBuffer[pos] = static_cast<unsigned char>(value | 0x80);
    if (value >= (1 << 7)) {
        (unsigned char &)_varintBuffer[++pos] = static_cast<unsigned char>((value >> 7) | 0x80);
        if (value >= (1 << 14)) {
            (unsigned char &)_varintBuffer[++pos] = static_cast<unsigned char>((value >> 14) | 0x80);
            if (value >= (1 << 21)) {
                (unsigned char &)_varintBuffer[++pos] = static_cast<unsigned char>((value >> 21) | 0x80);
                if (value >= (1 << 28)) {
                    (unsigned char &)_varintBuffer[++pos] = static_cast<unsigned char>(value >> 28);
                    pos++;
                } else {
                    (unsigned char &)_varintBuffer[pos++] &= 0x7F;
                }
            } else {
                (unsigned char &)_varintBuffer[pos++] &= 0x7F;
            }
        } else {
            (unsigned char &)_varintBuffer[pos++] &= 0x7F;
        }
    } else {
        (unsigned char &)_varintBuffer[pos++] &= 0x7F;
    }

    dataStr.append(_varintBuffer, _varintBuffer + pos);
}

void SchemaWriter::writeBytes(string &dataStr, const string &value) { dataStr.append(value.begin(), value.end()); }

void SchemaWriter::writeBool(string &dataStr, bool value) {
    if (value) {
        writeLength(dataStr, (uint32_t)1);
    } else {
        writeLength(dataStr, (uint32_t)0);
    }
}

void SchemaWriter::writeHeader(string &dataStr) {
    char buf[8] = {0};
    SchemaHeader header(_version);
    header.toBufVersion(buf);
    dataStr.append(buf + 4, 4);
}

} // namespace common
} // namespace swift
