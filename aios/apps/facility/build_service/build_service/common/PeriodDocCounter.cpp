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
#include "build_service/common/PeriodDocCounter.h"

#include <cstddef>

using namespace std;

namespace build_service { namespace common {

void PeriodDocCounterHelper::shutdown()
{
    PeriodDocCounter<int8_t>::GetInstance()->shutdown();
    PeriodDocCounter<uint8_t>::GetInstance()->shutdown();
    PeriodDocCounter<int16_t>::GetInstance()->shutdown();
    PeriodDocCounter<uint16_t>::GetInstance()->shutdown();
    PeriodDocCounter<int32_t>::GetInstance()->shutdown();
    PeriodDocCounter<uint32_t>::GetInstance()->shutdown();
    PeriodDocCounter<int64_t>::GetInstance()->shutdown();
    PeriodDocCounter<uint64_t>::GetInstance()->shutdown();
    PeriodDocCounter<float>::GetInstance()->shutdown();
    PeriodDocCounter<double>::GetInstance()->shutdown();
    PeriodDocCounter<std::string>::GetInstance()->shutdown();
}

PeriodDocCounterBase* PeriodDocCounterHelper::create(FieldType ft)
{
    if (ft == FieldType::ft_int8) {
        return PeriodDocCounter<int8_t>::GetInstance();
    } else if (ft == FieldType::ft_uint8) {
        return PeriodDocCounter<uint8_t>::GetInstance();
    } else if (ft == FieldType::ft_int16) {
        return PeriodDocCounter<int16_t>::GetInstance();
    } else if (ft == FieldType::ft_uint16) {
        return PeriodDocCounter<uint16_t>::GetInstance();
    } else if (ft == FieldType::ft_int32) {
        return PeriodDocCounter<int32_t>::GetInstance();
    } else if (ft == FieldType::ft_uint32) {
        return PeriodDocCounter<uint32_t>::GetInstance();
    } else if (ft == FieldType::ft_int64) {
        return PeriodDocCounter<int64_t>::GetInstance();
    } else if (ft == FieldType::ft_uint64) {
        return PeriodDocCounter<uint64_t>::GetInstance();
    } else if (ft == FieldType::ft_float) {
        return PeriodDocCounter<float>::GetInstance();
    } else if (ft == FieldType::ft_double) {
        return PeriodDocCounter<double>::GetInstance();
    } else if (ft == FieldType::ft_string) {
        return PeriodDocCounter<std::string>::GetInstance();
    }
    return nullptr;
}

void PeriodDocCounterHelper::count(PeriodDocCounterType type, FieldType ft, const void* data, size_t n,
                                   PeriodDocCounterBase* counter)
{
    if (counter == NULL) {
        return;
    }
    if (ft == FieldType::ft_int8) {
        (static_cast<PeriodDocCounter<int8_t>*>(counter))->count(type, *(int8_t*)data);
    } else if (ft == FieldType::ft_uint8) {
        (static_cast<PeriodDocCounter<uint8_t>*>(counter))->count(type, *(uint8_t*)(data));
    } else if (ft == FieldType::ft_int16) {
        (static_cast<PeriodDocCounter<int16_t>*>(counter))->count(type, *(int16_t*)(data));
    } else if (ft == FieldType::ft_uint16) {
        (static_cast<PeriodDocCounter<uint16_t>*>(counter))->count(type, *(uint16_t*)(data));
    } else if (ft == FieldType::ft_int32) {
        (static_cast<PeriodDocCounter<int32_t>*>(counter))->count(type, *(int32_t*)(data));
    } else if (ft == FieldType::ft_uint32) {
        (static_cast<PeriodDocCounter<uint32_t>*>(counter))->count(type, *(uint32_t*)(data));
    } else if (ft == FieldType::ft_int64) {
        (static_cast<PeriodDocCounter<int64_t>*>(counter))->count(type, *(int64_t*)(data));
    } else if (ft == FieldType::ft_uint64) {
        (static_cast<PeriodDocCounter<uint64_t>*>(counter))->count(type, *(uint64_t*)(data));
    } else if (ft == FieldType::ft_float) {
        (static_cast<PeriodDocCounter<float>*>(counter))->count(type, *(float*)(data));
    } else if (ft == FieldType::ft_double) {
        (static_cast<PeriodDocCounter<double>*>(counter))->count(type, *(double*)(data));
    } else if (ft == FieldType::ft_string) {
        string buffer((const char*)data, n);
        (static_cast<PeriodDocCounter<string>*>(counter))->count(type, buffer);
    }
}

void PeriodDocCounterHelper::count(PeriodDocCounterType type, const string& data, PeriodDocCounterBase* counter)
{
    if (counter == NULL || data.empty()) {
        return;
    }
    (static_cast<PeriodDocCounter<string>*>(counter))->count(type, data);
}

}} // namespace build_service::common
