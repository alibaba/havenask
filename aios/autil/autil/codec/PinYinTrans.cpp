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
/**
 * File name: pinyintrans.cpp
 * Author: zhixu.zt
 * Create time: 2014-01-05 16:57:51
 */

#include "autil/codec/PinYinTrans.h"

#include <fstream>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/codec/StringUtil.h"
#include "autil/codec/UTF8Util.h"

namespace autil {
namespace codec {
AUTIL_LOG_SETUP(autil::codec, PinYinTrans);

PinYinTrans::PinYinTrans() {}

PinYinTrans::~PinYinTrans() {}

int32_t PinYinTrans::init(const std::string &pinyin_file) {
    if (pinyin_file.empty()) {
        AUTIL_LOG(ERROR, "PINYIN_TRANS conf file empty.");
        return -1;
    }

    static const std::string true_string = "true";
    std::ifstream if_pinyin(pinyin_file.c_str());
    if (!if_pinyin) {
        AUTIL_LOG(ERROR, "pinyin_trans file [%s] is not exist.", pinyin_file.c_str());
        return -1;
    }

    uint64_t hash_key;
    std::string line;
    std::vector<std::string> pinyin_vec;
    std::vector<std::string> multi_yin;
    while (getline(if_pinyin, line)) {
        PinYinTrans::PINYIN pinyin;
        StringUtil::split(pinyin_vec, line, '\t', 4);
        if (pinyin_vec.size() < 4 || pinyin_vec[0].empty()) {
            continue;
        }

        // TODO(Unkown): GetValueOfCharUTF8 只能支持单个中文
        hash_key = hashKey(pinyin_vec[0].c_str(), pinyin_vec[0].length());
        pinyin.chinese = pinyin_vec[0];
        pinyin.quanpin = pinyin_vec[1];
        pinyin.jianpin = pinyin_vec[2];
        pinyin.repeat = pinyin_vec[3] == true_string ? true : false;
        if (_pinyin_trans_map.find(hash_key) != _pinyin_trans_map.end()) {
            AUTIL_LOG(DEBUG, "repeart hanzi : %s , ignore.", line.c_str());
            continue;
        }
        _pinyin_trans_map[hash_key] = pinyin;
    }
    if (_pinyin_trans_map.size() < 10000) {
        AUTIL_LOG(WARN, "中文转换词表过少 : %s : %lu.", pinyin_file.c_str(), _pinyin_trans_map.size());
    }
    return 0;
}

int32_t PinYinTrans::getQuanPin(const std::string &str, std::vector<std::string> &res, bool single) {
    if (str.empty()) {
        return -1;
    }

    std::string word;
    std::string::size_type start = 0;
    uint64_t hash_key = 0;
    while ((word = UTF8Util::getNextCharUTF8(str, start)) != "") {
        if (3 == word.length()) {
            // utf8下一个汉字占3Bype
            hash_key = hashKey(word.c_str(), word.length());
            PinYinMapItr itr = _pinyin_trans_map.find(hash_key);
            if (itr != _pinyin_trans_map.end()) {
                if (itr->second.repeat) {
                    appendMultipleRes(itr->second.quanpin, res, single);
                } else {
                    appendSolitionRes(itr->second.quanpin, res);
                }
            } else {
                // 如果没有对应的全拼，直接用中文
                appendSolitionRes(word, res);
            }
        } else {
            // 非汉字的都直接解析
            appendSolitionRes(word, res);
        }
        start += word.length();
    }
    if (start != str.length())
        return -1;
    return 0;
}

int32_t PinYinTrans::getJianPin(const std::string &str, std::vector<std::string> &res, bool single) {
    if (str.empty()) {
        return -1;
    }

    std::string word;
    std::string::size_type start = 0;
    uint64_t hash_key = 0;
    while ((word = UTF8Util::getNextCharUTF8(str, start)) != "") {
        if (3 == word.length()) {
            // utf8下一个汉字占3Bype
            hash_key = hashKey(word.c_str(), word.length());
            PinYinMapItr itr = _pinyin_trans_map.find(hash_key);
            if (itr != _pinyin_trans_map.end()) {
                if (itr->second.repeat) {
                    appendMultipleRes(itr->second.jianpin, res, single);
                } else {
                    appendSolitionRes(itr->second.jianpin, res);
                }
            } else {
                // 如果没有对应的全拼，直接用中文
                appendSolitionRes(word, res);
            }
        } else {
            // 非汉字的都直接解析
            appendSolitionRes(word, res);
        }
        start += word.length();
    }
    if (start != str.length())
        return -1;
    return 0;
}

int32_t PinYinTrans::getPinYin(const std::string &str,
                               std::vector<std::string> &quanpin,
                               std::vector<std::string> &jianpin,
                               bool single) {
    if (str.empty()) {
        return -1;
    }

    std::string word;
    std::string::size_type start = 0;
    uint64_t hash_key = 0;
    while ((word = UTF8Util::getNextCharUTF8(str, start)) != "") {
        if (3 == word.length()) {
            // utf8下一个汉字占3Bype
            hash_key = hashKey(word.c_str(), word.length());
            PinYinMapItr itr = _pinyin_trans_map.find(hash_key);
            if (itr != _pinyin_trans_map.end()) {
                if (itr->second.repeat) {
                    appendMultipleRes(itr->second.quanpin, quanpin, single);
                    appendMultipleRes(itr->second.jianpin, jianpin, single);
                } else {
                    appendSolitionRes(itr->second.quanpin, quanpin);
                    appendSolitionRes(itr->second.jianpin, jianpin);
                }
            } else {
                // 如果没有对应的全拼，直接用中文
                appendSolitionRes(word, quanpin);
                appendSolitionRes(word, jianpin);
            }
        } else {
            // 非汉字的都直接解析
            appendSolitionRes(word, quanpin);
            appendSolitionRes(word, jianpin);
        }
        start += word.length();
    }
    if (start != str.length())
        return -1;
    return 0;
}

uint64_t PinYinTrans::hashKey(const char *str, size_t len) {
    uint64_t hash = 0;
    for (size_t i = 0; i < len; i++) {
        hash = ((hash << 8) | ((uint8_t)str[i]));
    }
    return hash;
}
} // namespace codec
} // namespace autil
