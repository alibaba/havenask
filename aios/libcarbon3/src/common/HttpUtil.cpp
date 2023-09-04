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
#include "HttpUtil.h"
#include "autil/StringUtil.h"
#include <cctype>
#include <iomanip>
#include <sstream>

BEGIN_CARBON_NAMESPACE(common);

using namespace std;
using namespace autil;

std::string HttpUtil::urlEncode(const std::string& value) {
    ostringstream escaped;
    escaped.fill('0');
    escaped << hex;

    for (string::const_iterator i = value.begin(), n = value.end(); i != n; ++i) {
        string::value_type c = (*i);
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
            continue;
        }
        escaped << uppercase;
        escaped << '%' << setw(2) << int((unsigned char) c);
        escaped << nouppercase;
    }
    return escaped.str();
}

std::string HttpUtil::encodeParams(const StringKVS& kvs) {
    std::string output;
    for (const auto& kv : kvs) {
        std::string s = kv.first + "=" + urlEncode(kv.second);
        if (output.empty()) {
            output = s;
        } else {
            output += "&" + s;
        }
    }
    return output;
}

std::string HttpUtil::fixHostSpec(const std::string& host) {
    std::string ret;
    size_t pos = 0;
    if (!StringUtil::startsWith(host, "tcp:")) {
        ret = "tcp:";
        pos = host.find(":");
    }
    if (host.find(":", pos) == std::string::npos) {
        ret += host + ":80";
    } else {
        ret += host;
    }
    return ret;
}

END_CARBON_NAMESPACE(common);
