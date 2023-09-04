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
#ifndef ISEARCH_MULTI_CALL_METAENV_H
#define ISEARCH_MULTI_CALL_METAENV_H

#include <string>

#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

class GigMetaEnv;

/**
 * meta info from env
 * pass to client as MetaNode proto
 **/
class MetaEnv
{
public:
    MetaEnv();
    ~MetaEnv();

public:
    bool init();
    bool operator==(const MetaEnv &rhs) const;
    std::map<std::string, std::string> getEnvTags() const;
    std::map<std::string, std::string> getTargetTags() const;

    const std::string getPlatform() const {
        return _platform;
    }
    const std::string getHippoCluster() const {
        return _hippoCluster;
    }
    const std::string getHippoApp() const {
        return _hippoApp;
    }
    const std::string getC2Role() const {
        return _c2role;
    }
    const std::string getC2Group() const {
        return _c2group;
    }

    void setPlatform(const std::string &p) {
        _platform = p;
    }
    void setHippoCluster(const std::string &c) {
        _hippoCluster = c;
    }
    void setHippoApp(const std::string &a) {
        _hippoApp = a;
    }
    void setC2Role(const std::string &r) {
        _c2role = r;
    }
    void setC2Group(const std::string &g) {
        _c2group = g;
    }

    bool valid() const;
    void setFromProto(const GigMetaEnv &gigMetaEnv);
    void fillProto(GigMetaEnv &gigMetaEnv) const;
    void toString(std::string &ret) const;

public:
    static bool isUnknown(const std::string &env);
    static std::string getFromEnv(const std::string &key);

private:
    std::string _platform;
    std::string _hippoCluster;
    std::string _hippoApp;
    std::string _c2role;
    std::string _c2group;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(MetaEnv);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_METAENV_H
