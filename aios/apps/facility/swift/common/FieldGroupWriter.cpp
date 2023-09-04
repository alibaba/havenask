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
#include "swift/common/FieldGroupWriter.h"

#include <cstddef>

using namespace std;
using namespace autil;
namespace swift {
namespace common {
AUTIL_LOG_SETUP(swift, FieldGroupWriter);

FieldGroupWriter::FieldGroupWriter() : _buf(DEFAULT_BUF_SIZE) {}

FieldGroupWriter::~FieldGroupWriter() {}

void FieldGroupWriter::reset() { _buf.reset(); }

void FieldGroupWriter::toString(string &str) { str.assign(_buf.getBuffer(), _buf.getDataLen()); }

string FieldGroupWriter::toString() {
    string tmpStr;
    toString(tmpStr);
    return tmpStr;
}

void FieldGroupWriter::addConsumptionField(const StringView *value, bool isUpdated) {
    bool isExisted = (NULL != value);
    writeBool(isExisted);
    if (isExisted) {
        writeLength(value->size());
        writeBytes(*value);
        writeBool(isUpdated);
    }
}

void FieldGroupWriter::addProductionField(const string &name, const string &value, bool isUpdated) {
    addProductionField(
        autil::StringView(name.c_str(), name.size()), autil::StringView(value.c_str(), value.size()), isUpdated);
}

void FieldGroupWriter::addProductionField(const autil::StringView &name,
                                          const autil::StringView &value,
                                          bool isUpdated) {
    uint32_t nameLen = name.size();
    writeLength(nameLen);
    writeBytes(name);

    uint32_t valueLen = value.size();
    writeLength(valueLen);
    writeBytes(value);

    writeBool(isUpdated);
}

void FieldGroupWriter::writeLength(uint32_t value) {
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

    _buf.add(_varintBuffer, pos);
}

void FieldGroupWriter::writeBytes(const StringView &value) { _buf.add(value.data(), value.size()); }

void FieldGroupWriter::writeBool(bool value) {
    if (value) {
        writeLength((uint32_t)1);
    } else {
        writeLength((uint32_t)0);
    }
}

} // namespace common
} // namespace swift
