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

#include "absl/strings/numbers.h"
#include "absl/strings/string_view.h"

namespace autil {

class NumbersUtil {
public:
    static bool safe_strtouint8_base(const char *word, size_t size, uint8_t &value, int base = 10);
    static bool safe_strtoint8_base(const char *word, size_t size, int8_t &value, int base = 10);
    static bool safe_strtouint16_base(const char *word, size_t size, uint16_t &value, int base = 10);
    static bool safe_strtoint16_base(const char *word, size_t size, int16_t &value, int base = 10);
    static bool safe_strtouint32_base(const char *word, size_t size, uint32_t &value, int base = 10);
    static bool safe_strtoint32_base(const char *word, size_t size, int32_t &value, int base = 10);
    static bool safe_strtouint64_base(const char *word, size_t size, uint64_t &value, int base = 10);
    static bool safe_strtoint64_base(const char *word, size_t size, int64_t &value, int base = 10);

private:
    NumbersUtil();
    ~NumbersUtil();
    NumbersUtil(const NumbersUtil &);
    NumbersUtil &operator=(const NumbersUtil &);
};

#define SAFE_STR_TO_NUMBERS_BASE(RET_BIT, TEMP_VAR_BIT)                                                                \
    inline bool NumbersUtil::safe_strtouint##RET_BIT##_base(                                                           \
        const char *word, size_t size, uint##RET_BIT##_t &value, int base) {                                           \
        uint##TEMP_VAR_BIT##_t var = 0;                                                                                \
        absl::string_view str(word, size);                                                                             \
        bool ret = absl::numbers_internal::safe_strtou##TEMP_VAR_BIT##_base(str, &var, base);                          \
        value = (uint##RET_BIT##_t)var;                                                                                \
        return ret && var <= UINT##RET_BIT##_MAX;                                                                      \
    }                                                                                                                  \
                                                                                                                       \
    inline bool NumbersUtil::safe_strtoint##RET_BIT##_base(                                                            \
        const char *word, size_t size, int##RET_BIT##_t &value, int base) {                                            \
        int##TEMP_VAR_BIT##_t var = 0;                                                                                 \
        absl::string_view str(word, size);                                                                             \
        bool ret = absl::numbers_internal::safe_strto##TEMP_VAR_BIT##_base(str, &var, base);                           \
        value = (int##RET_BIT##_t)var;                                                                                 \
        return ret && var <= INT##RET_BIT##_MAX;                                                                       \
    }

SAFE_STR_TO_NUMBERS_BASE(8, 32);
SAFE_STR_TO_NUMBERS_BASE(16, 32);
SAFE_STR_TO_NUMBERS_BASE(32, 32);
SAFE_STR_TO_NUMBERS_BASE(64, 64);

} // namespace autil
