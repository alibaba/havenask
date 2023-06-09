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
#include "indexlib/index/common/SortValueConvertor.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/common/FieldTypeTraits.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SortValueConvertor);

SortValueConvertor::ConvertFunc SortValueConvertor::GenerateConvertor(config::SortPattern sp, FieldType type)
{
    switch (type) {
    case ft_int8:
        if (sp == config::sp_asc)
            return Convert<int8_t, config::sp_asc>;
        else
            return Convert<int8_t, config::sp_desc>;
        break;
    case ft_uint8:
        if (sp == config::sp_asc)
            return Convert<uint8_t, config::sp_asc>;
        else
            return Convert<uint8_t, config::sp_desc>;
        break;
    case ft_int16:
        if (sp == config::sp_asc)
            return Convert<int16_t, config::sp_asc>;
        else
            return Convert<int16_t, config::sp_desc>;
        break;
    case ft_uint16:
        if (sp == config::sp_asc)
            return Convert<uint16_t, config::sp_asc>;
        else
            return Convert<uint16_t, config::sp_desc>;
        break;
    case ft_integer:
        if (sp == config::sp_asc)
            return Convert<int32_t, config::sp_asc>;
        else
            return Convert<int32_t, config::sp_desc>;
        break;
    case ft_uint32:
    case ft_date:
    case ft_time:
        if (sp == config::sp_asc)
            return Convert<uint32_t, config::sp_asc>;
        else
            return Convert<uint32_t, config::sp_desc>;
        break;
    case ft_long:
        if (sp == config::sp_asc)
            return Convert<int64_t, config::sp_asc>;
        else
            return Convert<int64_t, config::sp_desc>;
        break;
    case ft_uint64:
    case ft_timestamp:
        if (sp == config::sp_asc)
            return Convert<uint64_t, config::sp_asc>;
        else
            return Convert<uint64_t, config::sp_desc>;
        break;
    case ft_float:
        if (sp == config::sp_asc)
            return Convert<float, config::sp_asc>;
        else
            return Convert<float, config::sp_desc>;
        break;
    case ft_double:
        if (sp == config::sp_asc)
            return Convert<double, config::sp_asc>;
        else
            return Convert<double, config::sp_desc>;
        break;
    case ft_hash_64:
        if (sp == config::sp_asc)
            return Convert<uint64_t, config::sp_asc>;
        else
            return Convert<uint64_t, config::sp_desc>;
        break;
    default:
        return nullptr;
    }
    return nullptr;
}

std::vector<std::pair<SortValueConvertor::ConvertFunc, size_t>>
SortValueConvertor::GenerateConvertors(const config::SortDescriptions& sortDesc,
                                       const std::shared_ptr<config::TabletSchema>& schema)
{
    std::vector<std::pair<ConvertFunc, size_t>> convertFuncs;
    for (auto& desc : sortDesc) {
        auto field = schema->GetFieldConfig(desc.GetSortFieldName());
        convertFuncs.push_back({GenerateConvertor(desc.GetSortPattern(), field->GetFieldType()),
                                indexlib::index::SizeOfFieldType(field->GetFieldType())});
    }
    return convertFuncs;
}

} // namespace indexlibv2::index
