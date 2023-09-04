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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/StringView.h"
#include "swift/common/Common.h"

namespace swift {
namespace common {
struct Field {
    autil::StringView name;
    autil::StringView value;
    bool isExisted;
    bool isUpdated;
    Field() : isExisted(true), isUpdated(false){};
};

class FieldGroupReader {
public:
    FieldGroupReader();
    ~FieldGroupReader();

private:
    FieldGroupReader(const FieldGroupReader &);
    FieldGroupReader &operator=(const FieldGroupReader &);

public:
    bool fromProductionString(const char *dataStr, size_t dataLen);
    bool fromProductionString(const std::string &dataStr);
    bool fromConsumptionString(const char *dataStr, size_t dataLen);
    bool fromConsumptionString(const std::string &dataStr);
    size_t getFieldSize() const { return _fieldVec.size(); }
    const Field *getField(size_t pos) const {
        if (pos >= _fieldVec.size()) {
            return NULL;
        } else {
            return &_fieldVec[pos];
        }
    }
    const Field *getField(const std::string &fieldName) const;

private:
    bool readLength(uint32_t &value);
    bool readBytes(uint32_t len, autil::StringView &cs);
    bool readBool(bool &updated);
    void reset(const char *dataStr, size_t dataLen);
    void reset(const std::string &dataStr);

private:
    uint32_t _dataLength;
    uint32_t _readCursor;
    const char *_data;
    std::vector<Field> _fieldVec;
    typedef std::vector<std::pair<size_t, uint32_t>> FieldHash2PosVec;
    mutable FieldHash2PosVec _fieldHash2PosVec;

private:
    friend class FieldGroupReaderTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FieldGroupReader);

} // namespace common
} // namespace swift
