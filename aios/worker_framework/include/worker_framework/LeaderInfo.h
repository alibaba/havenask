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

#include "autil/StringUtil.h"

namespace worker_framework {

class LeaderInfo {
public:
    LeaderInfo() {}
    ~LeaderInfo() {}

private:
    LeaderInfo(const LeaderInfo &);
    LeaderInfo &operator=(const LeaderInfo &);

public:
    std::string toString() const {
        std::string content = "{\n";
        std::map<std::string, std::string>::const_iterator it = _kv.begin();
        while (it != _kv.end()) {
            content += "\t";
            content += "\"" + it->first + "\" : ";
            content += it->second;
            if (++it == _kv.end()) {
                break;
            }
            content += ",\n";
        }
        content += "\n}\n";
        return content;
    }

    template <typename value_type>
    void set(const std::string &key, const value_type &value) {
        _kv[key] = autil::StringUtil::toString(value);
    }

    void set(const std::string &key, const std::string &value) {
        _kv[key] = "\"" + autil::StringUtil::toString(value) + "\"";
    }

    void setHttpAddress(const std::string &ip, uint16_t port) {
        set("httpAddress", ip + ":" + autil::StringUtil::toString(port));
        set("httpPort", port);
    }

private:
    std::map<std::string, std::string> _kv;
};

typedef std::shared_ptr<LeaderInfo> LeaderInfoPtr;

}; // namespace worker_framework
