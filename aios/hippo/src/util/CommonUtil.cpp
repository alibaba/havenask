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
#include "util/CommonUtil.h"
#include <math.h>
#include <numeric>
#include <algorithm>

using namespace std;
using namespace autil;
using namespace rapidjson;
BEGIN_HIPPO_NAMESPACE(util);

HIPPO_LOG_SETUP(util, CommonUtil);

CommonUtil::CommonUtil() {
}

CommonUtil::~CommonUtil() {
}

#define GEN_STRING(in, cascader, out)                   \
    if (in.empty()) {                                   \
        return "";                                      \
    }                                                   \
    out = "";                                           \
    for (auto it = in.begin(); it != in.end(); ++it) {  \
        out += StringUtil::toString(*it) + cascader;    \
    }                                                   \
    out = out.substr(0, out.size() - cascader.size());  \
    return out;                                         \
    

string CommonUtil::toString(const vector<uint32_t> &in, const string &cascader) {
    string out;
    GEN_STRING(in, cascader, out);
}

string CommonUtil::toString(const vector<int32_t> &in, const string &cascader) {
    string out;
    GEN_STRING(in, cascader, out);    
}


string CommonUtil::toString(const vector<string> &in, const string &cascader) {
    string out;
    GEN_STRING(in, cascader, out);    
}

string CommonUtil::toString(const set<int32_t> &in, const string &cascader) {
    string out;
    GEN_STRING(in, cascader, out);        
}

string CommonUtil::toString(const set<uint32_t> &in, const string &cascader) {
    string out;
    GEN_STRING(in, cascader, out);        
}

string CommonUtil::toString(const set<string> &in, const string &cascader) {
    string out;
    GEN_STRING(in, cascader, out);        
}

std::string CommonUtil::toString(const std::map<string, string> &in,
                                 const string &kvCascader,
                                 const string &cascader)
{
    if (in.empty()) {
        return "";
    }
    string out = "";
    map<string, string>::const_iterator it = in.begin();
    for (; it != in.end(); ++it) {
        out += autil::StringUtil::toString(it->first) + kvCascader +
               autil::StringUtil::toString(it->second) + cascader;
    }
    out = out.substr(0, out.size() - 1);
    return out;
}

string CommonUtil::uniqString(const std::string &appId,
                              const std::string &tag)
{
    return appId + "_" + tag + "_" +
        autil::StringUtil::toString(appId.size())
        + "_" + autil::StringUtil::toString(tag.size());
}

std::string CommonUtil::boolToString(bool in) {
    if (in) {
        return string("true");
    } else {
        return string("false");
    }
}

bool CommonUtil::stringToBool(const std::string &in,
                              bool defaultValue)
{
    if (in == "true") {
        return true;
    } else if (in == "false") {
        return false;
    } else {
        return defaultValue;
    }
}

void CommonUtil::transAddressToIp(std::string &address) {
    size_t pos = address.find_first_of(':');
    if (pos == string::npos) {
        return;
    }
    address = address.substr(0, pos);
}

string CommonUtil::getIpFromAddress(const std::string &address) {
    size_t pos = address.find_first_of(':');
    if (pos != string::npos) {
        return address.substr(0, pos);
    }
    return address;
}

bool CommonUtil::getOptionValue(const KeyValueMap& configs,
                                const string &option,
                                string &value) 
{
    auto it = configs.find(option);
    if (it == configs.end()) {
        return false;
    }
    value = it->second;
    return true;
}

bool CommonUtil::isOptionEnabled(const KeyValueMap &configs,
                                 const string &option)
{
    auto it = configs.find(option);
    if (it == configs.end()) {
        return false;
    }
    if (it->second != CONFIG_ENABLE) {
        return false;
    }
    return true;
}

bool CommonUtil::isSubMap(const KeyValueMap &parent,
                          const KeyValueMap &child)
{
    for (const auto &e: child) {
        auto it = parent.find(e.first);
        if (it == parent.end() || it->second != e.second) {
            return false;
        }
    }
    return true;
}

int64_t CommonUtil::getTodaySpecificSeconds(int hour, int min) {
    time_t lt = time(NULL);
    struct tm tim;
    localtime_r(&lt, &tim);
    tim.tm_sec = 0;
    tim.tm_hour = hour;
    tim.tm_min = min;
    int64_t t = (int64_t)mktime(&tim);
    return t;
}

void CommonUtil::replaceAllString(string &str, const string& oldStr, const string& newStr) {
    while (true) {
        size_t pos = str.rfind(oldStr);
        if (pos != string::npos) {
            str.replace(pos, oldStr.size(), newStr);
        } else {
            break;
        }
    }
}

string CommonUtil::currentTimeStringRFC3339() {
    time_t lt = time(NULL);
    struct tm tim;
    localtime_r(&lt, &tim);
    char str[200];
    size_t len = strftime(str, sizeof(str), "%FT%T%z", &tim);
    //format %z hhmm -> hh:mm
    if (len > 1) {
        char minute[] = { str[len-2], str[len-1], '\0' };
        sprintf(str + len - 2, ":%s", minute);
    }
    return string(str);
}

string CommonUtil::formatTimeStringRFC3339(const time_t seconds) {
    struct tm tim;
    localtime_r(&seconds, &tim);
    char str[200];
    size_t len = strftime(str, sizeof(str), "%FT%T%z", &tim);
    //format %z hhmm -> hh:mm
    if (len > 1) {
        char minute[] = { str[len-2], str[len-1], '\0' };
        sprintf(str + len - 2, ":%s", minute);
    }
    return string(str);
}
int64_t CommonUtil::RFC3339StringTimeToSeconds(const std::string &strTime){
    //2019-10-24T15:52:27+08:00 -> 2019-10-24T15:52:27+0800
    if(strTime.empty()) {
        return 0;
    }
    string tmp = strTime;
    size_t len = tmp.size();
    if(len > 3 && tmp[len - 3] == ':') {
        tmp[len-3]=tmp[len-2];
        tmp[len-2]=tmp[len-1];
        tmp[len - 1]='\0';
    }
    cout<<tmp<<endl;
    struct tm tm = {0};
    strptime(tmp.c_str(), "%FT%T%z", &tm);
    time_t timestamp = mktime(&tm);
    int64_t ts = timestamp;
    return ts;
}

string CommonUtil::formatTimeStringRFC3339(const string &timeString) {
    // remove content from . to +
    // 2019-08-05T10:43:08.420328257+08:00 -> 2019-08-05T10:43:08+08:00
    size_t cFind = timeString.find('.');
    size_t pFind = timeString.find('+');
    if (cFind != string::npos && pFind != string::npos && cFind < pFind) {
        return timeString.substr(0, cFind) + timeString.substr(pFind);
    }
    return timeString;
}

string CommonUtil::normalizeKmonitorTag(string tag) {
    string tempTag = tag;
    replaceAllString(tempTag, "/", ".");
    return tempTag;
}

bool CommonUtil::parseCurrentEnvsAndLabels(
        const Document &doc, KeyValueMap &envs, KeyValueMap &labels)
{
    if (!doc.HasMember("Config") || !doc["Config"].IsObject()) {
        return false;
    }
    if (!doc["Config"].HasMember("Env") || !doc["Config"]["Env"].IsArray() ||
        !doc["Config"].HasMember("Labels") || !doc["Config"]["Labels"].IsObject())
    {
        return false;
    }
    for (auto it = doc["Config"]["Env"].Begin();
         it != doc["Config"]["Env"].End(); ++it)
    {
        const string &v = it->GetString();
        size_t pos = v.find_first_of("=");
        if (pos == string::npos) {
            continue;
        }
        string key = v.substr(0, pos);
        envs[key] = v.substr(pos + 1);
    }
    for (auto it = doc["Config"]["Labels"].MemberBegin();
         it != doc["Config"]["Labels"].MemberEnd(); ++it)
    {
        labels[it->name.GetString()] = it->value.GetString();
    }
    return true;
}

int64_t CommonUtil::calcMultiThreadBatchCount(int32_t taskNum,
        int32_t threadNum)
{
    assert(threadNum > 0);
    return (taskNum + (threadNum - 1)) / threadNum;
}

bool CommonUtil::parseCurrentMounts(
            const rapidjson::Document &doc, KeyValueMap &mounts){
    if (!doc.HasMember("Mounts")) {
        return false;
    }
    for (auto it = doc["Mounts"].Begin();
         it != doc["Mounts"].End(); ++it)
    {
        string key = it->FindMember("Source")->value.GetString();
        string value = it->FindMember("Destination")->value.GetString();
        mounts[value] = key;
    }
    return true;
}

/*
  the former is combined with a header of 8 bytes and a payload
  see: xxxx://invalid/pouch/pouch/blob/06.18.2019/docs/api/HTTP_API.md
  header := [8]byte{STREAM_TYPE, 0, 0, 0, SIZE1, SIZE2, SIZE3, SIZE4}
*/
bool CommonUtil::parseStreamContent(
        const string &streamInput, string &stdout, string &stderr)
{
    if (streamInput.empty()) {
        return true;
    }
    size_t streamLength = streamInput.size();
    if (streamLength <= (size_t)8) {
        HIPPO_LOG(ERROR, "parse stream content:[%s] error!",
                  streamInput.c_str());
        return false;
    }
    size_t pos = 0;
    while (pos < streamLength) {
        uint32_t type = StringUtil::deserializeUInt32(
                streamInput.substr(pos, 1));
        uint32_t length = StringUtil::deserializeUInt32(
                streamInput.substr(pos + 4, 4));
        pos += 8;
        if (pos + (size_t)length > streamLength) {
            HIPPO_LOG(ERROR, "length[%u] at pos[%zd] is invalid. content:[%s]",
                      length, pos - 4, streamInput.c_str());
            return false;
        }
        if (type == 1) {
            stdout += streamInput.substr(pos, length);
        } else if (type == 2) {
            stderr += streamInput.substr(pos, length);
        }
        pos += length;
    }
    return true;
}

bool CommonUtil::calculateMeanAndVariance(
        const vector<int32_t> &temp, double &mean, double &stdev)
{
    if(temp.size() < 2) {
        return false;
    }
    double sum = std::accumulate(std::begin(temp), std::end(temp), 0.0);
    mean =  sum / temp.size(); //均值
    double accum  = 0.0;
    std::for_each (std::begin(temp), std::end(temp), [&](const double d) {
		accum  += (d-mean)*(d-mean);
	});
    stdev = sqrt(accum/(temp.size()-1)); //方差
    return true;
}

void CommonUtil::increaseMap(map<string, int32_t> &value,
                             string key, int32_t step, int32_t defaultValue)
{
    auto it = value.find(key);
    if(it == value.end()) {
        value[key] = step + defaultValue;
    } else {
        value[key] = it->second + step;
    }
}

int32_t CommonUtil::convert_10_to_8(int32_t num){
    char buf[64]={0};
    sprintf(buf, "%o", num);
    return atoi(buf);
}
//CPU hippo not support binSuffixes 1:1024
void CommonUtil::unitTransformCPU(const std::string& value, int64_t& v){
    auto suffix = value.rbegin();
    if (*suffix == 'i') {
        ++suffix;
    }
    int64_t tv = 0;
    StringUtil::strToInt64(value.c_str(), tv);
    v = tv;
    if (*suffix == 'n') {
        v = (tv/1000/1000)/10;
    }else if (*suffix == 'u') {
        v = (tv/1000)/10;
    }else if (*suffix == 'm') {
        v = tv/10;
    }else if (*suffix == 'k' || *suffix == 'K') {
        v = (tv*1000) * 100;
    }else if (*suffix == 'M') {
        v = (tv*1000*1000) * 100;
    }else if (*suffix == 'G') {
        v = (tv*1000*1000*1000) * 100;
    }else if (*suffix == 'T') {
        v = (tv*1000*1000*1000*1000) * 100;
    }else if (*suffix == 'P') {
        v = (tv*1000*1000*1000*1000*1000) * 100;
    }else if (*suffix == 'E') {
        v = (tv*1000*1000*1000*1000*1000*1000) * 100;
    }else{
        v = tv * 100;
    }
}

void CommonUtil::unitTransform(const string& value, int64_t& v) {
    bool decimal = true;
    auto rIt = value.rbegin();
    if (*rIt == 'i') {
        decimal = false;
        ++rIt;
    }
    int64_t tv = 0;
    StringUtil::strToInt64(value.c_str(), tv);
    v = tv;
    if (*rIt == 'M') {
        if (decimal) {
            v = (tv*1000*1000) >> 20;
        }
    } else if (*rIt == 'G') {
        if (decimal) {
            v = (tv*1000*1000*1000) >> 20;
        } else {
            v = tv << 10;
        }
    } else if (*rIt == 'K' || *rIt == 'k') {
        if (decimal) {
            v = (tv*1000) >> 20;
        } else {
            v = tv >> 10;
        }
    } else if (*rIt == 'E') {
        if (decimal) {
            v = (tv*1000*1000*1000*1000*1000*1000) >> 20;
        } else {
            v = tv << 40;
        }
    } else if (*rIt == 'P') {
        if (decimal) {
            v = (tv*1000*1000*1000*1000*1000) >> 20;
        } else {
            v = tv << 30;
        }
    } else if (*rIt == 'T') {
        if (decimal) {
            v = (tv*1000*1000*1000*1000) >> 20;
        } else {
            v = tv << 20;
        }
    }else{
        //default byte
        v = tv >> 20;
    }
}

void CommonUtil::addToStrStrIntMap(const string &key1, const string &key2,
                                   const int32_t value, StrStrIntMap &valueMap)
{
    if (value <= 0) {
        return;
    }
    auto it1 = valueMap.find(key1);
    if (it1 == valueMap.end() ||
        it1->second.find(key2) == it1->second.end())
    {
        valueMap[key1][key2] = 0;
    }
    valueMap[key1][key2] += value;
}

END_HIPPO_NAMESPACE(util);

