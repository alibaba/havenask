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

#include "autil/Log.h"
#include "indexlib/base/BinaryStringUtil.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/TabletSchema.h"

namespace indexlibv2::index {

class SortValueConvertor
{
public:
    SortValueConvertor() = delete;
    ~SortValueConvertor() = delete;

public:
    using ConvertFunc = std::function<std::string(const autil::StringView& sortValue, bool isNull)>;

public:
    template <typename T, config::SortPattern SP>
    static std::string Convert(const autil::StringView& sortValueString, bool isNull)
    {
        // ignore when length not match (due to null or empty)
        T value = sortValueString.length() == sizeof(T) ? *((T*)sortValueString.data()) : 0;
        if constexpr (SP == config::sp_asc) {
            return BinaryStringUtil::toString(value, isNull);
        } else {
            return BinaryStringUtil::toInvertString(value, isNull);
        }
    }

    static std::vector<std::pair<ConvertFunc, size_t>>
    GenerateConvertors(const config::SortDescriptions& sortDesc, const std::shared_ptr<config::TabletSchema>& schema);

    static ConvertFunc GenerateConvertor(config::SortPattern sp, FieldType type);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
