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
#include "indexlib/index/orc/ColumnBuffer.h"

#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/index/orc/MultiStringColumnBuffer.h"
#include "indexlib/index/orc/MultiValueColumnBuffer.h"
#include "indexlib/index/orc/SingleValueColumnBuffer.h"
#include "indexlib/index/orc/StringColumnBuffer.h"
#include "indexlib/index/orc/TypeUtils.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, ColumnBuffer);

ColumnBuffer::ColumnBuffer(const config::FieldConfig* field, size_t batchSize) : _field(field), _batchSize(batchSize) {}

std::unique_ptr<ColumnBuffer> ColumnBuffer::Create(const config::FieldConfig* field, size_t batchSize,
                                                   orc::ColumnVectorBatch* batch)
{
#define INIT_BUFFER_AND_RETURN(buffer)                                                                                 \
    Status s = buffer->Init(batch);                                                                                    \
    if (s.IsOK()) {                                                                                                    \
        return std::move(buffer);                                                                                      \
    } else {                                                                                                           \
        return nullptr;                                                                                                \
    }

#define CASE(ft, field, batchSize)                                                                                     \
    case ft: {                                                                                                         \
        typedef indexlib::IndexlibFieldType2CppType<ft, false>::CppType CppType;                                       \
        if (field->IsMultiValue()) {                                                                                   \
            auto buffer = std::make_unique<MultiValueColumnBuffer<CppType>>(field, batchSize);                         \
            INIT_BUFFER_AND_RETURN(buffer);                                                                            \
        } else {                                                                                                       \
            auto buffer = std::make_unique<SingleValueColumnBuffer<CppType>>(field, batchSize);                        \
            INIT_BUFFER_AND_RETURN(buffer);                                                                            \
        }                                                                                                              \
    } break
    switch (field->GetFieldType()) {
        CASE(FieldType::ft_int8, field, batchSize);
        CASE(FieldType::ft_int16, field, batchSize);
        CASE(FieldType::ft_int32, field, batchSize);
        CASE(FieldType::ft_int64, field, batchSize);
        CASE(FieldType::ft_uint8, field, batchSize);
        CASE(FieldType::ft_uint16, field, batchSize);
        CASE(FieldType::ft_uint32, field, batchSize);
        CASE(FieldType::ft_uint64, field, batchSize);
        CASE(FieldType::ft_double, field, batchSize);
        CASE(FieldType::ft_float, field, batchSize);
    case FieldType::ft_string: {
        if (field->IsMultiValue()) {
            auto buffer = std::make_unique<MultiStringColumnBuffer>(field, batchSize);
            INIT_BUFFER_AND_RETURN(buffer);
        } else {
            auto buffer = std::make_unique<StringColumnBuffer>(field, batchSize);
            INIT_BUFFER_AND_RETURN(buffer);
        }
    } break;
    default:
        return nullptr;
    }
}

} // namespace indexlibv2::index
