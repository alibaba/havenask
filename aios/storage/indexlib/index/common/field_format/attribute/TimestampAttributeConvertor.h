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
#include "indexlib/index/common/field_format/attribute/SingleValueAttributeConvertor.h"

namespace indexlibv2::index {

class TimestampAttributeConvertor : public SingleValueAttributeConvertor<uint64_t>
{
public:
    TimestampAttributeConvertor(bool needHash, const std::string& fieldName, int32_t defaultTimeZoneDelta);
    ~TimestampAttributeConvertor() = default;

public:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& strResult, char* outBuffer, EncodeStatus& status) override;

private:
    int32_t _defaultTimeZoneDelta;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
