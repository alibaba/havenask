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
#include "indexlib/util/Base62.h"

#include <boost/multiprecision/cpp_int.hpp>
#include <cmath>
#include <cstdint>
#include <exception>
#include <stddef.h>

#include "boost/core/enable_if.hpp"
#include "boost/multiprecision/detail/default_ops.hpp"
#include "boost/multiprecision/detail/et_ops.hpp"
#include "boost/multiprecision/detail/number_base.hpp"
#include "boost/multiprecision/detail/number_compare.hpp"
#include "boost/multiprecision/number.hpp"

namespace indexlibv2::util {

static char charset[] = "0123456789"
                        "abcdefghijklmnopqrstuvwxyz"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

using cpp_int = boost::multiprecision::cpp_int;

Status Base62::EncodeInteger(uint64_t num, std::string& str)
{
    std::string tmp;
    do {
        tmp.push_back(charset[num % 62]);
        num /= 62;
    } while (num != 0);
    str = std::string(tmp.rbegin(), tmp.rend());
    return Status::OK();
}

Status Base62::EncodeInteger(const std::string& numStr, std::string& str)
{
    size_t leadZeroBytes = 0;
    while (leadZeroBytes < numStr.size()) {
        if (numStr[leadZeroBytes] == '0' && leadZeroBytes != (numStr.size() - 1)) {
            leadZeroBytes++;
        } else {
            break;
        }
    }
    std::string trimmedStr = numStr.substr(leadZeroBytes, numStr.size());
    try {
        auto num = cpp_int(trimmedStr);
        if (num < 0) {
            return Status::InvalidArgs("input num [%s] is negative", trimmedStr.c_str());
        }
        std::string tmp;
        do {
            tmp.push_back(charset[int(num % 62)]);
            num /= 62;
        } while (num != 0);
        str = std::string(tmp.rbegin(), tmp.rend());
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgs("input num [%s] invalid, Exception[%s]", trimmedStr.c_str(), e.what());
    } catch (...) {
        return Status::InvalidArgs("input num [%s] invalid, unknown exception", trimmedStr.c_str());
    }
}

Status Base62::DecodeInteger(const std::string& str, uint64_t& num)
{
    if (str.empty()) {
        return Status::InvalidArgs("invalid empty str.");
    }
    uint64_t res = 0;
    for (size_t i = 0; i < str.size(); i++) {
        int idx = FindIdx(str[i]);
        if (idx == -1) {
            return Status::InvalidArgs("str [%s] container invalid char [%c]", str.c_str(), str[i]);
        }
        uint64_t power = str.size() - (i + 1);
        res += uint64_t(std::pow(62, power)) * idx;
    }
    num = res;
    return Status::OK();
}

Status Base62::DecodeInteger(const std::string& str, std::string& num)
{
    if (str.empty()) {
        return Status::InvalidArgs("invalid empty str.");
    }
    cpp_int res = 0;
    for (size_t i = 0; i < str.size(); i++) {
        int idx = FindIdx(str[i]);
        if (idx == -1) {
            return Status::InvalidArgs("str [%s] container invalid char [%c]", str.c_str(), str[i]);
        }
        uint64_t power = str.size() - (i + 1);
        res += cpp_int(boost::multiprecision::pow(cpp_int(62), power)) * idx;
    }
    num = res.str();
    return Status::OK();
}

int Base62::FindIdx(char c)
{
    int idx = -1;
    for (size_t j = 0; j < 62; j++) {
        if (c == charset[j]) {
            idx = j;
            break;
        }
    }
    return idx;
}

} // namespace indexlibv2::util
