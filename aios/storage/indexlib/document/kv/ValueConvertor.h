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
#include "indexlib/base/Status.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::config {
class ValueConfig;
class FieldConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {
class AttributeConvertor;
class PackAttributeFormatter;
class PackAttributeConfig;
} // namespace indexlibv2::index

namespace indexlibv2::document {

class RawDocument;

class ValueConvertor
{
public:
    struct ConvertResult {
        ConvertResult() {}
        ConvertResult(autil::StringView convertedValue, bool hasFormatError, bool success)
            : convertedValue(convertedValue)
            , hasFormatError(hasFormatError)
            , success(success)
        {
        }
        autil::StringView convertedValue;
        bool hasFormatError = false;
        bool success = false;
    };

public:
    ValueConvertor(bool parseDelete = false);
    ~ValueConvertor();

    Status Init(const std::shared_ptr<config::ValueConfig>& valueConfig, bool forcePackValue);
    ConvertResult ConvertValue(const RawDocument& rawDoc, autil::mem_pool::Pool* pool);

private:
    autil::StringView ConvertField(const RawDocument& rawDoc, const config::FieldConfig& fieldConfig,
                                   bool emptyFieldNotEncode, autil::mem_pool::Pool* pool, bool* hasFormatError) const;

private:
    bool _parseDelete;
    std::shared_ptr<config::ValueConfig> _valueConfig;
    std::shared_ptr<index::PackAttributeConfig> _packAttrConfig;
    std::vector<std::unique_ptr<index::AttributeConvertor>> _fieldConvertors; // indexed by fieldId
    std::unique_ptr<index::PackAttributeFormatter> _packFormatter;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlibv2::document
