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
#include "indexlib/common/field_format/attribute/time_attribute_convertor.h"

#include "indexlib/util/TimestampUtil.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, TimeAttributeConvertor);

TimeAttributeConvertor::TimeAttributeConvertor(bool needHash, const string& fieldName)
    : SingleValueAttributeConvertor<uint32_t>(needHash, fieldName)
{
}

TimeAttributeConvertor::~TimeAttributeConvertor() {}

StringView TimeAttributeConvertor::InnerEncode(const StringView& attrData, Pool* memPool, string& strResult,
                                               char* outBuffer, EncodeStatus& status)
{
    assert(mNeedHash == false);
    uint32_t* value = (uint32_t*)allocate(memPool, strResult, outBuffer, sizeof(uint32_t));

    uint64_t timestamp;
    if (!TimestampUtil::ConvertToTimestamp(ft_time, attrData, timestamp, 0)) {
        status = ES_TYPE_ERROR;
        *value = uint32_t();
        IE_LOG(DEBUG, "convert attribute [%s] invalid time format [%s].", mFieldName.c_str(), attrData.data());
        ERROR_COLLECTOR_LOG(ERROR, "convert attribute [%s] invalid time format [%s].", mFieldName.c_str(),
                            attrData.data());
    } else {
        *value = (uint32_t)timestamp;
        ;
    }
    return StringView((char*)value, sizeof(uint32_t));
}
}} // namespace indexlib::common
