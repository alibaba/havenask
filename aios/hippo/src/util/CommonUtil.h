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
#ifndef HIPPO_COMMONUTIL_H
#define HIPPO_COMMONUTIL_H

#include <list>
#include "util/Log.h"
#include "common/common.h"
#include "autil/StringUtil.h"
#include <rapidjson/document.h>

BEGIN_HIPPO_NAMESPACE(util);
class CommonUtil
{
public:
    CommonUtil();
    ~CommonUtil();
private:
    CommonUtil(const CommonUtil &);
    CommonUtil& operator=(const CommonUtil &);
public:
    static std::string toString(const std::vector<uint32_t> &in,
                                const std::string &cascader = ",");
    
    static std::string toString(const std::vector<int32_t> &in,
                                const std::string &cascader = ",");
    
    static std::string toString(const std::vector<std::string> &in,
                                const std::string &cascader = ",");
    
    static std::string toString(const std::set<int32_t> &in,
                                const std::string &cascader = ",");

    static std::string toString(const std::set<uint32_t> &in,
                                const std::string &cascader = ",");
    
    static std::string toString(const std::set<std::string> &in,
                                const std::string &cascader = ",");

    template <typename T>
    static std::string toString(const std::list<T> &in,
                                const std::string &cascader = ",");

    
    static std::string toString(const KeyValueMap &in,
                                const std::string& kvCascader = ":",
                                const std::string& cascader = ",");

    template <typename T1, typename T2>
    static std::string getMapKeyString(const std::map<T1, T2>& in,
            const std::string& cascader = ",");
    
    template <typename T1, typename T2>
    static std::string toString(const std::map<T1, T2>& in,
                                const std::string& kvCascader = "=",
                                const std::string& cascader = ",");

    static std::string uniqString(const std::string &appId,
                                  const std::string &tag);
    
    static std::string boolToString(bool in);
    
    static bool stringToBool(const std::string &in,
                             bool defaultValue = false);

    static void transAddressToIp(std::string &address);

    static std::string getIpFromAddress(const std::string &address); 
    
    static bool getOptionValue(const KeyValueMap& configs,
                               const std::string &option,
                               std::string &value);

    static bool isOptionEnabled(const KeyValueMap &configs,
                                const std::string &option);

    static void  increaseMap(std::map<std::string, int32_t> &value, 
			     std::string key, int32_t step, int32_t defaultValue = 0);

    static bool isSubMap(const KeyValueMap &parent,
                         const KeyValueMap &child);
    static int64_t getTodaySpecificSeconds(int hour, int min);

    static void replaceAllString(std::string &str,
                                 const std::string& oldStr,
                                 const std::string& newStr);
    static std::string currentTimeStringRFC3339();

    static std::string formatTimeStringRFC3339(const time_t seconds);

    static std::string formatTimeStringRFC3339(const std::string &timeString);
    static int64_t RFC3339StringTimeToSeconds(const std::string &strTime);
    static std::string normalizeKmonitorTag(std::string);

    static bool parseCurrentEnvsAndLabels(
            const rapidjson::Document &doc, KeyValueMap &envs, KeyValueMap &labels);
    
    static bool parseCurrentMounts(
            const rapidjson::Document &doc, KeyValueMap &mounts);

    // 与CAS: CompareAndSwap不一样。
    // 当curr == expect时 return false
    // 当curr != expect时， 设置curr的值，并return true.
    template <typename T>
    static bool CompareAndSet(T& curr, const T& expect);

    static int64_t calcMultiThreadBatchCount(int32_t taskNum, int32_t threadNum);

    static bool parseStreamContent(const std::string &streamInput,
                                   std::string &stdout, std::string &stderr);

    static bool calculateMeanAndVariance(
            const std::vector<int32_t> &temp, double &mean, double &stdev);

    static int32_t convert_10_to_8(int32_t num);

    template <typename T1, typename T2>
    static bool comparePairValue(
            const std::pair<T1, T2> &lhs, const std::pair<T1, T2> &rhs);

    static void unitTransformCPU(const std::string& value, int64_t& v);

    static void unitTransform(const std::string& value, int64_t& v);

    static void addToStrStrIntMap(
            const std::string &key1, const std::string &key2,
            const int32_t value, StrStrIntMap &valueMap);

private:
    HIPPO_LOG_DECLARE();
};

template <typename T>
std::string CommonUtil::toString(const std::list<T>& in,
                                 const std::string& cascader)
{
    if (in.empty()) {
        return "";
    }
    std::string out = "";
    for (auto &it : in) {
        out += autil::StringUtil::toString(it) + cascader;
    }
    out = out.substr(0, out.size() - 1);
    return out;
}

template <typename T1, typename T2>
std::string CommonUtil::getMapKeyString(const std::map<T1, T2>& in,
                                        const std::string& cascader)
{
    StringSet keys;
    for (auto &e : in) {
        keys.insert(autil::StringUtil::toString(e.first));
    }
    return toString(keys, cascader);
}

template <typename T1, typename T2>
std::string CommonUtil::toString(const std::map<T1, T2>& in,
                                 const std::string& kvCascader,
                                 const std::string& cascader)
{
    if (in.empty()) {
        return "";
    }
    std::string out = "";
    for (auto &it : in) {
        out += autil::StringUtil::toString(it.first) + kvCascader +
               autil::StringUtil::toString(it.second) + cascader;
    }
    out = out.substr(0, out.size() - 1);
    return out;
}

template <typename T>
bool CommonUtil::CompareAndSet(T& curr, const T& expect) {
    if (curr == expect) {
        return false;
    }
    curr = expect;
    return true;
}

template <typename T1, typename T2>
bool CommonUtil::comparePairValue(
        const std::pair<T1, T2> &lhs, const std::pair<T1, T2> &rhs)
{
    return lhs.second < rhs.second;
}

HIPPO_INTERNAL_TYPEDEF_PTR(CommonUtil);

END_HIPPO_NAMESPACE(util);

#endif //HIPPO_COMMONUTIL_H
