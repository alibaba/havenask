/**
 * @file   knn_util.h
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Tue Jan 22 10:01:42 2019
 *
 * @brief
 *
 *
 */

#ifndef __INDEXLIB_AITHETA_UTIL_PARAM_UTIL_H_
#define __INDEXLIB_AITHETA_UTIL_PARAM_UTIL_H_

#include <autil/StringUtil.h>
#include <indexlib/common_define.h>
#include <indexlib/util/key_value_map.h>
#include <indexlib/misc/log.h>

IE_NAMESPACE_BEGIN(aitheta_plugin);

class ParamUtil {
 public:
    ParamUtil();
    ~ParamUtil();

 public:
    static void MergeParams(const util::KeyValueMap &srcParams, util::KeyValueMap &params, bool allowCover = false);

    template <typename T>
    static bool ExtractValue(const IE_NAMESPACE(util)::KeyValueMap &parameters, const std::string &key, T *val);

    template <typename T, typename... Args>
    static void ExtractValue(const IE_NAMESPACE(util)::KeyValueMap &parameters, const std::string &key, T *val,
                             Args... args);

 private:
    IE_LOG_DECLARE();
};

template <typename T>
bool ParamUtil::ExtractValue(const IE_NAMESPACE(util)::KeyValueMap &parameters, const std::string &key, T *val) {
    auto iterator = parameters.find(key);
    if (iterator == parameters.end()) {
        return false;
    }

    if (typeid(*val) == typeid(char)) {
        if (iterator->second.size() != 1) {
            IE_LOG(WARN, "[%s] should include and only include one char", iterator->second.data());
            return false;
        }
        char *cval = reinterpret_cast<char *>(val);
        *cval = iterator->second.front();
    } else {
        autil::StringUtil::fromString(iterator->second, *val);
    }
    return true;
}

template <typename T, typename... Args>
void ParamUtil::ExtractValue(const IE_NAMESPACE(util)::KeyValueMap &parameters, const std::string &key, T *val,
                             Args... args) {
    ExtractValue(parameters, key, val);
    ExtractValue(parameters, args...);
}

IE_NAMESPACE_END(aitheta_plugin);

#endif  // __INDEXLIB_AITHETA_UTIL_PARAM_UTIL_H_
