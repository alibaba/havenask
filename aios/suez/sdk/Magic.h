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

#define MAGIC_PP_MAP(macro, data, ...)                                                                                 \
    MAGIC_ID(MAGIC_APPLY(MAGIC_PP_MAP_VAR_COUNT, MAGIC_PP_COUNT(__VA_ARGS__))(macro, data, __VA_ARGS__))

#define MAGIC_PP_MAP_VAR_COUNT(count) MAGIC_M##count

#define MAGIC_APPLY(macro, ...) MAGIC_ID(macro(__VA_ARGS__))

#define MAGIC_ID(x) x

#define MAGIC_M1(m, d, x) m(d, 0, x)
#define MAGIC_M2(m, d, x, ...) m(d, 1, x) MAGIC_ID(MAGIC_M1(m, d, __VA_ARGS__))
#define MAGIC_M3(m, d, x, ...) m(d, 2, x) MAGIC_ID(MAGIC_M2(m, d, __VA_ARGS__))
#define MAGIC_M4(m, d, x, ...) m(d, 3, x) MAGIC_ID(MAGIC_M3(m, d, __VA_ARGS__))
#define MAGIC_M5(m, d, x, ...) m(d, 4, x) MAGIC_ID(MAGIC_M4(m, d, __VA_ARGS__))
#define MAGIC_M6(m, d, x, ...) m(d, 5, x) MAGIC_ID(MAGIC_M5(m, d, __VA_ARGS__))
#define MAGIC_M7(m, d, x, ...) m(d, 6, x) MAGIC_ID(MAGIC_M6(m, d, __VA_ARGS__))
#define MAGIC_M8(m, d, x, ...) m(d, 7, x) MAGIC_ID(MAGIC_M7(m, d, __VA_ARGS__))
#define MAGIC_M9(m, d, x, ...) m(d, 8, x) MAGIC_ID(MAGIC_M8(m, d, __VA_ARGS__))
#define MAGIC_M10(m, d, x, ...) m(d, 9, x) MAGIC_ID(MAGIC_M9(m, d, __VA_ARGS__))
#define MAGIC_M11(m, d, x, ...) m(d, 10, x) MAGIC_ID(MAGIC_M10(m, d, __VA_ARGS__))
#define MAGIC_M12(m, d, x, ...) m(d, 11, x) MAGIC_ID(MAGIC_M11(m, d, __VA_ARGS__))
#define MAGIC_M13(m, d, x, ...) m(d, 12, x) MAGIC_ID(MAGIC_M12(m, d, __VA_ARGS__))
#define MAGIC_M14(m, d, x, ...) m(d, 13, x) MAGIC_ID(MAGIC_M13(m, d, __VA_ARGS__))
#define MAGIC_M15(m, d, x, ...) m(d, 14, x) MAGIC_ID(MAGIC_M14(m, d, __VA_ARGS__))
#define MAGIC_M16(m, d, x, ...) m(d, 15, x) MAGIC_ID(MAGIC_M15(m, d, __VA_ARGS__))
#define MAGIC_M17(m, d, x, ...) m(d, 16, x) MAGIC_ID(MAGIC_M16(m, d, __VA_ARGS__))
#define MAGIC_M18(m, d, x, ...) m(d, 17, x) MAGIC_ID(MAGIC_M17(m, d, __VA_ARGS__))
#define MAGIC_M19(m, d, x, ...) m(d, 18, x) MAGIC_ID(MAGIC_M18(m, d, __VA_ARGS__))
#define MAGIC_M20(m, d, x, ...) m(d, 19, x) MAGIC_ID(MAGIC_M19(m, d, __VA_ARGS__))
#define MAGIC_M21(m, d, x, ...) m(d, 20, x) MAGIC_ID(MAGIC_M20(m, d, __VA_ARGS__))
#define MAGIC_M22(m, d, x, ...) m(d, 21, x) MAGIC_ID(MAGIC_M21(m, d, __VA_ARGS__))
#define MAGIC_M23(m, d, x, ...) m(d, 22, x) MAGIC_ID(MAGIC_M22(m, d, __VA_ARGS__))
#define MAGIC_M24(m, d, x, ...) m(d, 23, x) MAGIC_ID(MAGIC_M23(m, d, __VA_ARGS__))
#define MAGIC_M25(m, d, x, ...) m(d, 24, x) MAGIC_ID(MAGIC_M24(m, d, __VA_ARGS__))
#define MAGIC_M26(m, d, x, ...) m(d, 25, x) MAGIC_ID(MAGIC_M25(m, d, __VA_ARGS__))
#define MAGIC_M27(m, d, x, ...) m(d, 26, x) MAGIC_ID(MAGIC_M26(m, d, __VA_ARGS__))
#define MAGIC_M28(m, d, x, ...) m(d, 27, x) MAGIC_ID(MAGIC_M27(m, d, __VA_ARGS__))
#define MAGIC_M29(m, d, x, ...) m(d, 28, x) MAGIC_ID(MAGIC_M28(m, d, __VA_ARGS__))
#define MAGIC_M30(m, d, x, ...) m(d, 29, x) MAGIC_ID(MAGIC_M29(m, d, __VA_ARGS__))
#define MAGIC_M31(m, d, x, ...) m(d, 30, x) MAGIC_ID(MAGIC_M30(m, d, __VA_ARGS__))
#define MAGIC_M32(m, d, x, ...) m(d, 31, x) MAGIC_ID(MAGIC_M31(m, d, __VA_ARGS__))
#define MAGIC_M33(m, d, x, ...) m(d, 32, x) MAGIC_ID(MAGIC_M32(m, d, __VA_ARGS__))
#define MAGIC_M34(m, d, x, ...) m(d, 33, x) MAGIC_ID(MAGIC_M33(m, d, __VA_ARGS__))
#define MAGIC_M35(m, d, x, ...) m(d, 34, x) MAGIC_ID(MAGIC_M34(m, d, __VA_ARGS__))
#define MAGIC_M36(m, d, x, ...) m(d, 35, x) MAGIC_ID(MAGIC_M35(m, d, __VA_ARGS__))
#define MAGIC_M37(m, d, x, ...) m(d, 36, x) MAGIC_ID(MAGIC_M36(m, d, __VA_ARGS__))
#define MAGIC_M38(m, d, x, ...) m(d, 37, x) MAGIC_ID(MAGIC_M37(m, d, __VA_ARGS__))
#define MAGIC_M39(m, d, x, ...) m(d, 38, x) MAGIC_ID(MAGIC_M38(m, d, __VA_ARGS__))
#define MAGIC_M40(m, d, x, ...) m(d, 39, x) MAGIC_ID(MAGIC_M39(m, d, __VA_ARGS__))
#define MAGIC_M41(m, d, x, ...) m(d, 40, x) MAGIC_ID(MAGIC_M40(m, d, __VA_ARGS__))
#define MAGIC_M42(m, d, x, ...) m(d, 41, x) MAGIC_ID(MAGIC_M41(m, d, __VA_ARGS__))
#define MAGIC_M43(m, d, x, ...) m(d, 42, x) MAGIC_ID(MAGIC_M42(m, d, __VA_ARGS__))
#define MAGIC_M44(m, d, x, ...) m(d, 43, x) MAGIC_ID(MAGIC_M43(m, d, __VA_ARGS__))
#define MAGIC_M45(m, d, x, ...) m(d, 44, x) MAGIC_ID(MAGIC_M44(m, d, __VA_ARGS__))
#define MAGIC_M46(m, d, x, ...) m(d, 45, x) MAGIC_ID(MAGIC_M45(m, d, __VA_ARGS__))
#define MAGIC_M47(m, d, x, ...) m(d, 46, x) MAGIC_ID(MAGIC_M46(m, d, __VA_ARGS__))
#define MAGIC_M48(m, d, x, ...) m(d, 47, x) MAGIC_ID(MAGIC_M47(m, d, __VA_ARGS__))
#define MAGIC_M49(m, d, x, ...) m(d, 48, x) MAGIC_ID(MAGIC_M48(m, d, __VA_ARGS__))
#define MAGIC_M50(m, d, x, ...) m(d, 49, x) MAGIC_ID(MAGIC_M49(m, d, __VA_ARGS__))
#define MAGIC_M51(m, d, x, ...) m(d, 50, x) MAGIC_ID(MAGIC_M50(m, d, __VA_ARGS__))
#define MAGIC_M52(m, d, x, ...) m(d, 51, x) MAGIC_ID(MAGIC_M51(m, d, __VA_ARGS__))
#define MAGIC_M53(m, d, x, ...) m(d, 52, x) MAGIC_ID(MAGIC_M52(m, d, __VA_ARGS__))
#define MAGIC_M54(m, d, x, ...) m(d, 53, x) MAGIC_ID(MAGIC_M53(m, d, __VA_ARGS__))
#define MAGIC_M55(m, d, x, ...) m(d, 54, x) MAGIC_ID(MAGIC_M54(m, d, __VA_ARGS__))
#define MAGIC_M56(m, d, x, ...) m(d, 55, x) MAGIC_ID(MAGIC_M55(m, d, __VA_ARGS__))
#define MAGIC_M57(m, d, x, ...) m(d, 56, x) MAGIC_ID(MAGIC_M56(m, d, __VA_ARGS__))
#define MAGIC_M58(m, d, x, ...) m(d, 57, x) MAGIC_ID(MAGIC_M57(m, d, __VA_ARGS__))
#define MAGIC_M59(m, d, x, ...) m(d, 58, x) MAGIC_ID(MAGIC_M58(m, d, __VA_ARGS__))
#define MAGIC_M60(m, d, x, ...) m(d, 59, x) MAGIC_ID(MAGIC_M59(m, d, __VA_ARGS__))
#define MAGIC_M61(m, d, x, ...) m(d, 60, x) MAGIC_ID(MAGIC_M60(m, d, __VA_ARGS__))
#define MAGIC_M62(m, d, x, ...) m(d, 61, x) MAGIC_ID(MAGIC_M61(m, d, __VA_ARGS__))
#define MAGIC_M63(m, d, x, ...) m(d, 62, x) MAGIC_ID(MAGIC_M62(m, d, __VA_ARGS__))
#define MAGIC_M64(m, d, x, ...) m(d, 63, x) MAGIC_ID(MAGIC_M63(m, d, __VA_ARGS__))

#define MAGIC_PP_COUNT_IMPL(_1,                                                                                        \
                            _2,                                                                                        \
                            _3,                                                                                        \
                            _4,                                                                                        \
                            _5,                                                                                        \
                            _6,                                                                                        \
                            _7,                                                                                        \
                            _8,                                                                                        \
                            _9,                                                                                        \
                            _10,                                                                                       \
                            _11,                                                                                       \
                            _12,                                                                                       \
                            _13,                                                                                       \
                            _14,                                                                                       \
                            _15,                                                                                       \
                            _16,                                                                                       \
                            _17,                                                                                       \
                            _18,                                                                                       \
                            _19,                                                                                       \
                            _20,                                                                                       \
                            _21,                                                                                       \
                            _22,                                                                                       \
                            _23,                                                                                       \
                            _24,                                                                                       \
                            _25,                                                                                       \
                            _26,                                                                                       \
                            _27,                                                                                       \
                            _28,                                                                                       \
                            _29,                                                                                       \
                            _30,                                                                                       \
                            _31,                                                                                       \
                            _32,                                                                                       \
                            _33,                                                                                       \
                            _34,                                                                                       \
                            _35,                                                                                       \
                            _36,                                                                                       \
                            _37,                                                                                       \
                            _38,                                                                                       \
                            _39,                                                                                       \
                            _40,                                                                                       \
                            _41,                                                                                       \
                            _42,                                                                                       \
                            _43,                                                                                       \
                            _44,                                                                                       \
                            _45,                                                                                       \
                            _46,                                                                                       \
                            _47,                                                                                       \
                            _48,                                                                                       \
                            _49,                                                                                       \
                            _50,                                                                                       \
                            _51,                                                                                       \
                            _52,                                                                                       \
                            _53,                                                                                       \
                            _54,                                                                                       \
                            _55,                                                                                       \
                            _56,                                                                                       \
                            _57,                                                                                       \
                            _58,                                                                                       \
                            _59,                                                                                       \
                            _60,                                                                                       \
                            _61,                                                                                       \
                            _62,                                                                                       \
                            _63,                                                                                       \
                            _64,                                                                                       \
                            count,                                                                                     \
                            ...)                                                                                       \
    count

#define MAGIC_PP_COUNT(...)                                                                                            \
    MAGIC_ID(MAGIC_PP_COUNT_IMPL(__VA_ARGS__,                                                                          \
                                 64,                                                                                   \
                                 63,                                                                                   \
                                 62,                                                                                   \
                                 61,                                                                                   \
                                 60,                                                                                   \
                                 59,                                                                                   \
                                 58,                                                                                   \
                                 57,                                                                                   \
                                 56,                                                                                   \
                                 55,                                                                                   \
                                 54,                                                                                   \
                                 53,                                                                                   \
                                 52,                                                                                   \
                                 51,                                                                                   \
                                 50,                                                                                   \
                                 49,                                                                                   \
                                 48,                                                                                   \
                                 47,                                                                                   \
                                 46,                                                                                   \
                                 45,                                                                                   \
                                 44,                                                                                   \
                                 43,                                                                                   \
                                 42,                                                                                   \
                                 41,                                                                                   \
                                 40,                                                                                   \
                                 39,                                                                                   \
                                 38,                                                                                   \
                                 37,                                                                                   \
                                 36,                                                                                   \
                                 35,                                                                                   \
                                 34,                                                                                   \
                                 33,                                                                                   \
                                 32,                                                                                   \
                                 31,                                                                                   \
                                 30,                                                                                   \
                                 29,                                                                                   \
                                 28,                                                                                   \
                                 27,                                                                                   \
                                 26,                                                                                   \
                                 25,                                                                                   \
                                 24,                                                                                   \
                                 23,                                                                                   \
                                 22,                                                                                   \
                                 21,                                                                                   \
                                 20,                                                                                   \
                                 19,                                                                                   \
                                 18,                                                                                   \
                                 17,                                                                                   \
                                 16,                                                                                   \
                                 15,                                                                                   \
                                 14,                                                                                   \
                                 13,                                                                                   \
                                 12,                                                                                   \
                                 11,                                                                                   \
                                 10,                                                                                   \
                                 9,                                                                                    \
                                 8,                                                                                    \
                                 7,                                                                                    \
                                 6,                                                                                    \
                                 5,                                                                                    \
                                 4,                                                                                    \
                                 3,                                                                                    \
                                 2,                                                                                    \
                                 1))

#define MAGIC_ONE_CASE(data, index, enum_value)                                                                        \
    case enum_value:                                                                                                   \
        return #enum_value;                                                                                            \
        break;

#define ENUM_TO_STR(type, ...)                                                                                         \
    static inline const char *enumToCStr(type enum_in) {                                                               \
        switch (enum_in) {                                                                                             \
            MAGIC_PP_MAP(MAGIC_ONE_CASE, ignored, __VA_ARGS__)                                                         \
        default:                                                                                                       \
            return "";                                                                                                 \
        }                                                                                                              \
    }
