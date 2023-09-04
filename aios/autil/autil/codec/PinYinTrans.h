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
/**
 * File name: pinyintrans.h
 * Author: zhixu.zt
 * Create time: 2014-01-05 16:57:51
 */

#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/codec/StringUtil.h"

#define AUTIL_CODE_MAX_PINYIN_REPEAT 16

namespace autil {
namespace codec {

class PinYinTrans {
public:
    typedef struct {
        bool repeat; // 多音字
        std::string chinese;
        std::string quanpin;
        std::string jianpin;
    } PINYIN;
    typedef std::map<uint64_t, PINYIN> PinYinMap;
    typedef std::map<uint64_t, PINYIN>::iterator PinYinMapItr;

public:
    PinYinTrans();
    ~PinYinTrans();

public:
    int32_t init(const std::string &pinyin_file);

    int32_t getQuanPin(const std::string &str, std::vector<std::string> &res, bool single = false);
    int32_t getJianPin(const std::string &str, std::vector<std::string> &res, bool single = false);
    int32_t getPinYin(const std::string &str,
                      std::vector<std::string> &quanpin,
                      std::vector<std::string> &jianpin,
                      bool single = false);

private:
    uint64_t hashKey(const char *str, size_t len);
    inline int32_t appendSolitionRes(const std::string &str, std::vector<std::string> &res);
    inline int32_t appendMultipleRes(const std::string &str, std::vector<std::string> &res, bool single);

private:
    PinYinMap _pinyin_trans_map;
    AUTIL_LOG_DECLARE();
};

int32_t PinYinTrans::appendSolitionRes(const std::string &str, std::vector<std::string> &res) {
    if (res.size() == 0) {
        res.push_back(str);
        return 0;
    }

    for (size_t i = 0; i < res.size(); i++) {
        res[i].append(str);
    }
    return 0;
}

int32_t PinYinTrans::appendMultipleRes(const std::string &str, std::vector<std::string> &res, bool single) {
    std::vector<std::string> w_py_vec;
    StringUtil::split(w_py_vec, str, ',');

    if (single) {
        return appendSolitionRes(w_py_vec[0], res);
    }

    if (res.size() == 0) {
        res = w_py_vec;
        return 0;
    }

    size_t w_py_count = w_py_vec.size();
    size_t res_count = res.size();
    size_t j = 0;
    std::vector<std::string> new_res_vec;

    // 对多音字转换限制
    size_t max_reate = AUTIL_CODE_MAX_PINYIN_REPEAT / res_count;
    max_reate = max_reate == 0 ? 1 : max_reate;
    if (w_py_count > max_reate) {
        w_py_count = max_reate;
    }

    for (size_t i = 0; i < w_py_count; i++) {
        j = 0;
        for (; j < res_count; j++) {
            std::string r = res[j];
            r.append(w_py_vec[i]);
            new_res_vec.push_back(r);
        }
    }
    res = new_res_vec;
    return 0;
}

} // namespace codec
} // namespace autil
