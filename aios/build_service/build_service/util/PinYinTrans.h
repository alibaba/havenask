/**
 * File name: pinyintrans.h
 * Author: zhixu.zt
 * Create time: 2014-01-05 16:57:51
 */

#ifndef ISEARCH_BS_PINYINTRANS_H_
#define ISEARCH_BS_PINYINTRANS_H_

#include <string>
#include <vector>
#include <map>

#include "build_service/util/Log.h"
#include "build_service/util/StringUtil.h"

#define ISEARCH_BS_MAX_PINYIN_REPEAT 16

namespace build_service {
namespace util {

class PinYinTrans {
 public:
    typedef struct {
        bool repeat;  // 多音字
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
    int32_t getPinYin(const std::string &str, std::vector<std::string> &quanpin, std::vector<std::string> &jianpin,
                      bool single = false);

 private:
    uint64_t hashKey(const char *str, size_t len);
    inline int32_t appendSolitionRes(const std::string &str, std::vector<std::string> &res);
    inline int32_t appendMultipleRes(const std::string &str, std::vector<std::string> &res, bool single);

 private:
    PinYinMap _pinyin_trans_map;
    BS_LOG_DECLARE();
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
    size_t max_reate = ISEARCH_BS_MAX_PINYIN_REPEAT / res_count;
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

}
}
#endif  // D2_FRAMEWORK_UTIL_PINYINTRANS_H_
