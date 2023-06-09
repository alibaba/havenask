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
#include "indexlib/common/field_format/attribute/timestamp_attribute_convertor.h"

#include "indexlib/util/TimestampUtil.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, TimestampAttributeConvertor);

TimestampAttributeConvertor::TimestampAttributeConvertor(bool needHash, const string& fieldName,
                                                         int32_t defaultTimeZoneDelta)
    : SingleValueAttributeConvertor<uint64_t>(needHash, fieldName)
    , mDefaultTimeZoneDelta(defaultTimeZoneDelta)
{
}

TimestampAttributeConvertor::~TimestampAttributeConvertor() {}

StringView TimestampAttributeConvertor::InnerEncode(const StringView& attrData, Pool* memPool, string& strResult,
                                                    char* outBuffer, EncodeStatus& status)
{
    assert(mNeedHash == false);
    uint64_t* value = (uint64_t*)allocate(memPool, strResult, outBuffer, sizeof(uint64_t));

    uint64_t timestamp;
    if (!TimestampUtil::ConvertToTimestamp(ft_timestamp, attrData, timestamp, mDefaultTimeZoneDelta)) {
        status = ES_TYPE_ERROR;
        *value = uint64_t();
        IE_LOG(DEBUG, "convert attribute [%s] invalid timestamp format [%s].", mFieldName.c_str(), attrData.data());
        ERROR_COLLECTOR_LOG(ERROR, "convert attribute [%s] invalid timestamp format [%s].", mFieldName.c_str(),
                            attrData.data());
    } else {
        *value = timestamp;
    }
    return StringView((char*)value, sizeof(uint64_t));
}
}} // namespace indexlib::common
