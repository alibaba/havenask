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

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/FieldConfig.h"
#include "orc/Vector.hh"

namespace indexlibv2::index {

class ColumnBuffer
{
public:
    ColumnBuffer(const config::FieldConfig* fieldConfig, uint64_t batchSize);
    virtual ~ColumnBuffer() = default;

    ColumnBuffer(const ColumnBuffer&) = delete;
    ColumnBuffer& operator=(const ColumnBuffer&) = delete;
    ColumnBuffer(ColumnBuffer&&) = delete;
    ColumnBuffer& operator=(ColumnBuffer&&) = delete;

    virtual Status Init(orc::ColumnVectorBatch* batch) = 0;
    virtual void Append(const autil::StringView& field, uint64_t rowIdx) = 0;
    virtual void Seal(uint64_t rowIdx) = 0;
    virtual void Reset() = 0;

    static std::unique_ptr<ColumnBuffer> Create(const config::FieldConfig* fieldConfig, uint64_t batchSize,
                                                orc::ColumnVectorBatch* batch);
    const std::string& GetFieldName() const { return _field->GetFieldName(); }
    fieldid_t GetFieldId() const { return _field->GetFieldId(); }

protected:
    const config::FieldConfig* _field;
    const uint64_t _batchSize;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
