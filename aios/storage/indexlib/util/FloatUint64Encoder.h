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

namespace indexlib { namespace util {

class FloatUint64Encoder
{
public:
    FloatUint64Encoder() {}
    ~FloatUint64Encoder() {}

public:
    static uint32_t FloatToUint32(float value)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
        return reinterpret_cast<uint32_t&>(value);
#pragma GCC diagnostic pop
    }

    static uint64_t DoubleToUint64(double value)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
        return reinterpret_cast<uint64_t&>(value);
#pragma GCC diagnostic pop
    }

    static float Uint32ToFloat(uint32_t value)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
        return reinterpret_cast<float&>(value);
#pragma GCC diagnostic pop
    }

    static double Uint64ToDouble(uint64_t value)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
        return reinterpret_cast<double&>(value);
#pragma GCC diagnostic pop
    }
};
}} // namespace indexlib::util
