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

#include "autil/DynamicBuf.h"
#include "autil/Log.h"
#include "autil/StringView.h"
#include "swift/common/Common.h"

namespace swift {
namespace common {

const uint32_t SWIFT_COMMON_VARINT32_MAX_SIZE = sizeof(uint32_t) + 1;

class FieldGroupWriter {
public:
    FieldGroupWriter();
    ~FieldGroupWriter();

private:
    FieldGroupWriter(const FieldGroupWriter &);
    FieldGroupWriter &operator=(const FieldGroupWriter &);

public:
    void addProductionField(const std::string &name, const std::string &value, bool isUpdated = false);
    void addProductionField(const autil::StringView &name, const autil::StringView &value, bool isUpdated = false);
    void addConsumptionField(const autil::StringView *value, bool isUpdated = false);
    void toString(std::string &str);
    std::string toString();
    void reset();
    bool empty() const { return _buf.getDataLen() == 0; }

private:
    void writeLength(uint32_t value);
    void writeBytes(const autil::StringView &value);
    void writeBool(bool value);

private:
    char _varintBuffer[SWIFT_COMMON_VARINT32_MAX_SIZE];
    autil::DynamicBuf _buf;

private:
    friend class FieldGroupReaderTest;

private:
    static constexpr size_t DEFAULT_BUF_SIZE = 1024;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FieldGroupWriter);

} // namespace common
} // namespace swift
