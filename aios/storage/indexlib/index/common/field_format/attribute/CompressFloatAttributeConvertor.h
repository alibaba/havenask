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
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"

namespace indexlibv2::index {

class CompressFloatAttributeConvertor : public MultiValueAttributeConvertor<float>
{
public:
    CompressFloatAttributeConvertor(indexlib::config::CompressTypeOption compressType, bool needHash,
                                    const std::string& fieldName, int32_t fixedValueCount,
                                    const std::string& separator);

    ~CompressFloatAttributeConvertor() = default;

protected:
    autil::StringView InnerEncode(const autil::StringView& originalData,
                                  const std::vector<autil::StringView>& attrDatas, autil::mem_pool::Pool* memPool,
                                  std::string& resultStr, char* outBuffer, EncodeStatus& status) override;

    bool DecodeLiteralField(const autil::StringView& str, std::string& value) override;

private:
    indexlib::config::CompressTypeOption _compressType;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
