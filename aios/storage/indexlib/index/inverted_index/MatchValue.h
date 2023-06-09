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

namespace indexlib {
enum MatchValueType { mv_int8, mv_uint8, mv_int16, mv_uint16, mv_int32, mv_uint32, mv_float, mv_unknown };
class MatchValue
{
public:
    int8_t GetInt8() { return int8; };
    void SetInt8(int8_t value) { int8 = value; };
    uint8_t GetUint8() { return uint8; };
    void SetUint8(uint8_t value) { uint8 = value; };
    int16_t GetInt16() { return int16; };
    void SetInt16(int16_t value) { int16 = value; };
    uint16_t GetUint16() { return uint16; };
    void SetUint16(uint16_t value) { uint16 = value; };
    int32_t GetInt32() { return int32; };
    void SetInt32(int32_t value) { int32 = value; };
    uint32_t GetUint32() { return uint32; };
    void SetUint32(uint32_t value) { uint32 = value; };
    float GetFloat() { return float32; };
    void SetFloat(float value) { float32 = value; };

private:
    union {
        int8_t int8;
        uint8_t uint8;
        int16_t int16;
        uint16_t uint16;
        int32_t int32;
        uint32_t uint32;
        float float32;
    };
};
typedef MatchValue matchvalue_t;
} // namespace indexlib

namespace indexlibv2 {
typedef indexlib::MatchValue matchvalue_t;
}
