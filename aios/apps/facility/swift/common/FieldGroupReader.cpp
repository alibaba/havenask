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
#include "swift/common/FieldGroupReader.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <tr1/functional>
#include <type_traits>

#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"

using namespace autil;
using namespace std;
namespace swift {
namespace common {
AUTIL_LOG_SETUP(swift, FieldGroupReader);

FieldGroupReader::FieldGroupReader() { reset(""); }

FieldGroupReader::~FieldGroupReader() {}

void FieldGroupReader::reset(const string &dataStr) { reset(dataStr.c_str(), dataStr.size()); }

void FieldGroupReader::reset(const char *dataStr, size_t dataLen) {
    _dataLength = dataLen;
    _readCursor = 0;
    _data = dataStr;
    _fieldVec.clear();
    _fieldHash2PosVec.clear();
}

bool FieldGroupReader::fromProductionString(const string &data) {
    return fromProductionString(data.c_str(), data.size());
}

bool FieldGroupReader::fromProductionString(const char *dataStr, size_t dataLen) {
    reset(dataStr, dataLen);
    uint32_t len = 0;
    while (_readCursor < _dataLength) {
        Field tmpField;
        if (readLength(len) && readBytes(len, tmpField.name) && readLength(len) && readBytes(len, tmpField.value) &&
            readBool(tmpField.isUpdated)) {
            _fieldVec.push_back(tmpField);
        } else {
            return false;
        }
    }
    return true;
}

bool FieldGroupReader::fromConsumptionString(const std::string &data) {
    return fromConsumptionString(data.c_str(), data.size());
}

bool FieldGroupReader::fromConsumptionString(const char *dataStr, size_t dataLen) {
    reset(dataStr, dataLen);
    uint32_t len = 0;
    while (_readCursor < _dataLength) {
        Field tmpField;
        if (!readBool(tmpField.isExisted)) {
            return false;
        }
        if (tmpField.isExisted &&
            !(readLength(len) && readBytes(len, tmpField.value) && readBool(tmpField.isUpdated))) {
            return false;
        }
        _fieldVec.push_back(tmpField);
    }
    return true;
}

const Field *FieldGroupReader::getField(const std::string &fieldName) const {
    if (_fieldHash2PosVec.empty() && !_fieldVec.empty()) {
        _fieldHash2PosVec.resize(_fieldVec.size());
        for (size_t i = 0; i < _fieldVec.size(); i++) {
            const Field &field = _fieldVec[i];
            size_t hashVal = std::tr1::_Fnv_hash::hash(field.name.data(), field.name.size());
            _fieldHash2PosVec[i] = make_pair(hashVal, i);
        }
        sort(_fieldHash2PosVec.begin(), _fieldHash2PosVec.end());
    }
    size_t fieldHash = std::tr1::_Fnv_hash::hash(fieldName.data(), fieldName.size());
    FieldHash2PosVec::const_iterator low;

    low = std::lower_bound(_fieldHash2PosVec.begin(), _fieldHash2PosVec.end(), make_pair(fieldHash, 0u));
    while (low != _fieldHash2PosVec.end() && low->first == fieldHash) {
        if (_fieldVec[low->second].name == fieldName) {
            return &_fieldVec[low->second];
        } else {
            ++low;
        }
    }
    return NULL;
}

bool FieldGroupReader::readLength(uint32_t &value) {
    if (_readCursor >= _dataLength) {
        return false;
    }
    if (!((unsigned char &)_data[_readCursor] & 0x80)) {
        value = (unsigned char &)_data[_readCursor++];
        return true;
    }

    uint32_t b = 0;
    uint32_t result = 0;
    for (uint32_t i = 0; i < 5; ++i) {
        if (_readCursor >= _dataLength) {
            return false;
        }
        b = (unsigned char &)_data[_readCursor++];
        uint32_t offset = i * 7;
        if (i < 4) {
            result |= (b & 0x7F) << offset;
        } else {
            result |= b << offset;
        }
        if (!(b & 0x80)) {
            value = result;
            return true;
        }
    }

    return false;
}

bool FieldGroupReader::readBytes(uint32_t len, StringView &cs) {
    if (_readCursor + len > _dataLength) {
        return false;
    }
    cs = {_data + _readCursor, len};
    _readCursor += len;
    return true;
}

bool FieldGroupReader::readBool(bool &updated) {
    if (_readCursor >= _dataLength) {
        return false;
    }
    uint32_t value = 0;
    if (!readLength(value)) {
        return false;
    }
    updated = value == 0 ? false : true;
    return true;
}

} // namespace common
} // namespace swift
