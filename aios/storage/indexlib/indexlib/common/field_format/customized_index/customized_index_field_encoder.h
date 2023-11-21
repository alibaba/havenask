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

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/FloatUint64Encoder.h"

namespace indexlib { namespace common {

class CustomizedIndexFieldEncoder
{
public:
    CustomizedIndexFieldEncoder(const config::IndexPartitionSchemaPtr& schema);
    ~CustomizedIndexFieldEncoder();

public:
    enum FloatFieldType { FLOAT, DOUBLE, UNKOWN };

    void Init(const config::IndexPartitionSchemaPtr& schema);
    bool IsFloatField(fieldid_t fieldId);
    void Encode(fieldid_t fieldId, const std::string& fieldValue, std::vector<dictkey_t>& dictKeys);
    bool NormalizeToUInt64(fieldid_t fieldId, const std::string& fieldValue, uint64_t& value);
    static FloatFieldType GetFloatFieldType(FieldType fieldType);

private:
    std::vector<FloatFieldType> mFieldVec;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexFieldEncoder);

inline bool CustomizedIndexFieldEncoder::NormalizeToUInt64(fieldid_t fieldId, const std::string& fieldValue,
                                                           uint64_t& value)
{
    FloatFieldType type = mFieldVec[fieldId];
    if (type == FLOAT) {
        float num = 0;
        if (!autil::StringUtil::fromString(fieldValue, num)) {
            IE_LOG(DEBUG, "field value [%s] is not float number.", fieldValue.c_str());
            return false;
        }
        value = util::FloatUint64Encoder::FloatToUint32(num);
        return true;
    }

    assert(type == DOUBLE);
    double num = 0;
    if (!autil::StringUtil::fromString(fieldValue, num)) {
        IE_LOG(DEBUG, "field value [%s] is not double number.", fieldValue.c_str());
        return false;
    }
    value = util::FloatUint64Encoder::DoubleToUint64(num);
    return true;
}

inline void CustomizedIndexFieldEncoder::Encode(fieldid_t fieldId, const std::string& fieldValue,
                                                std::vector<dictkey_t>& dictKeys)
{
    dictKeys.clear();
    if (!IsFloatField(fieldId)) {
        return;
    }
    if (fieldValue.empty()) {
        IE_LOG(DEBUG, "field [%d] value is empty.", fieldId);
        return;
    }

    uint64_t value = 0;
    if (!NormalizeToUInt64(fieldId, fieldValue, value)) {
        return;
    }
    dictKeys.push_back(value);
}

inline bool CustomizedIndexFieldEncoder::IsFloatField(fieldid_t fieldId)
{
    if (fieldId >= (fieldid_t)mFieldVec.size()) {
        return false;
    }
    return mFieldVec[fieldId] != UNKOWN;
}
}} // namespace indexlib::common
