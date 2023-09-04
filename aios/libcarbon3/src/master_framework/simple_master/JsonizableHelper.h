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
#ifndef MASTER_FRAMEWORK_JSONIZABLEHELPER_H
#define MASTER_FRAMEWORK_JSONIZABLEHELPER_H

#include "common/Log.h"
#include "master_framework/common.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

template <typename T>
void typedJsonize(T& a, autil::legacy::Jsonizable::JsonWrapper &json) {
    assert(false);
}
    
template <typename T>
class JsonizeWrapper : public autil::legacy::Jsonizable {
public:
    JsonizeWrapper() {}
    JsonizeWrapper(const T &t) {
        a = t;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        typedJsonize<T>(a, json);
    }
    
    T a;
};

template <typename T>
void jsonizeVector(const std::string &name,
                   std::vector<T> &vect,
                   autil::legacy::Jsonizable::JsonWrapper &json)
{
    if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
        std::vector<JsonizeWrapper<T> > jsonizeWrapperVect;
        for (size_t i = 0; i < vect.size(); i++) {
            jsonizeWrapperVect.push_back(JsonizeWrapper<T>(vect[i]));
        }
        json.Jsonize(name, jsonizeWrapperVect);
    } else {
        std::vector<JsonizeWrapper<T> > jsonizeWrapperVect;
        json.Jsonize(name, jsonizeWrapperVect, jsonizeWrapperVect);
        vect.clear();
        for (size_t i = 0; i < jsonizeWrapperVect.size(); i++) {
            vect.push_back(jsonizeWrapperVect[i].a);
        }
    }
}

template <>
void jsonizeVector<hippo::SlotResource>(const std::string &name,
                                        std::vector<hippo::SlotResource> &vect,
                                        autil::legacy::Jsonizable::JsonWrapper &json)
{
    if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
        std::vector<std::vector<JsonizeWrapper<hippo::Resource> > > jsonizeWrapperVect;
        for (size_t i = 0; i < vect.size(); i++) {
            std::vector<JsonizeWrapper<hippo::Resource> > resourceVect;
            for (size_t j = 0; j < vect[i].resources.size(); j++) {
                resourceVect.push_back(JsonizeWrapper<hippo::Resource>(
                                vect[i].resources[j]));
            }
            jsonizeWrapperVect.push_back(resourceVect);
        }
        json.Jsonize(name, jsonizeWrapperVect, jsonizeWrapperVect);
    } else {
        std::vector<std::vector<JsonizeWrapper<hippo::Resource> > > jsonizeWrapperVect;
        json.Jsonize(name, jsonizeWrapperVect, jsonizeWrapperVect);
        vect.clear();
        for (size_t i = 0; i < jsonizeWrapperVect.size(); i++) {
            hippo::SlotResource slotResource;
            for (size_t j = 0; j < jsonizeWrapperVect[i].size(); j++) {
                slotResource.resources.push_back(jsonizeWrapperVect[i][j].a);
            }
            vect.push_back(slotResource);
        }
    }
}

std::string convertResourceType(hippo::Resource::Type type) {
    if (type == hippo::Resource::SCALAR) {
        return "SCALAR";
    } else if (type == hippo::Resource::TEXT) {
        return "TEXT";
    } else {
        return "EXCLUDE_TEXT";
    }
}

hippo::Resource::Type convertResourceType(const std::string &type) {
    if (type == "SCALAR") {
        return hippo::Resource::SCALAR;
    } else if (type == "TEXT") {
        return hippo::Resource::TEXT;
    } else {
        return hippo::Resource::EXCLUDE_TEXT;
    }
}

template <>
void typedJsonize<hippo::Resource>(
        hippo::Resource &a,
        autil::legacy::Jsonizable::JsonWrapper &json)
{
    json.Jsonize("name", a.name);
    json.Jsonize("amount", a.amount, 0);
    if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
        std::string typeStr = convertResourceType(a.type); 
        json.Jsonize("type", typeStr);
    } else {
        std::string typeStr;
        json.Jsonize("type", typeStr, "SCALAR");
        a.type = convertResourceType(typeStr);
    }
};

template <>
void typedJsonize<hippo::DataInfo>(
        hippo::DataInfo &a, autil::legacy::Jsonizable::JsonWrapper &json)
{
    json.Jsonize("name", a.name);
    json.Jsonize("src", a.src);
    json.Jsonize("dst", a.dst);
    json.Jsonize("version", a.version, a.version);
    json.Jsonize("retryCountLimit", a.retryCountLimit, a.retryCountLimit);
    json.Jsonize("attemptId", a.attemptId, a.attemptId);
    json.Jsonize("expireTime", a.expireTime, a.expireTime);    
};

void pairsToMaps(const std::vector<std::pair<std::string, std::string> > &pairs,
                 std::vector<std::map<std::string, std::string> > &kvs)
{
    kvs.clear();
    for (size_t i = 0; i < pairs.size(); i++) {
        std::map<std::string, std::string> m;
        m["key"] = pairs[i].first;
        m["value"] = pairs[i].second;
        kvs.push_back(m);
    }
}

void mapsToPairs(
        const std::vector<std::map<std::string, std::string> > &kvs,
        std::vector<std::pair<std::string, std::string> > &pairs)
{
    pairs.clear();
    for (size_t i = 0; i < kvs.size(); i++) {
        std::map<std::string, std::string> m = kvs[i];
        pairs.push_back(make_pair(m["key"], m["value"]));
    }
}

template <>
void typedJsonize<hippo::ProcessInfo>(
        hippo::ProcessInfo &a,
        autil::legacy::Jsonizable::JsonWrapper &json)
{
    json.Jsonize("isDaemon", a.isDaemon, true);
    json.Jsonize("processName", a.name, a.name);
    json.Jsonize("cmd", a.cmd, a.cmd);
    json.Jsonize("instanceId", a.instanceId, a.instanceId);

    if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
        std::vector<std::map<std::string, std::string> > argsMaps;
        pairsToMaps(a.args, argsMaps);
        json.Jsonize("args", argsMaps);
        
        std::vector<std::map<std::string, std::string> > envsMaps;
        pairsToMaps(a.envs, envsMaps);
        json.Jsonize("envs", envsMaps);
    } else {
        std::vector<std::map<std::string, std::string> > argsMaps;
        json.Jsonize("args", argsMaps, argsMaps);
        mapsToPairs(argsMaps, a.args);
        
        std::vector<std::map<std::string, std::string> >envsMaps;        
        json.Jsonize("envs", envsMaps, envsMaps);
        mapsToPairs(envsMaps, a.envs);
    }
};

std::string convertPackageType(const hippo::PackageInfo::PackageType &type) {
    switch(type) {
    case hippo::PackageInfo::UNSUPPORTED:
        return "UNSUPPORTED";
    case hippo::PackageInfo::RPM:
        return "RPM";
    case hippo::PackageInfo::ARCHIVE:
        return "ARCHIVE";
    default:
        return "";
    }
}

hippo::PackageInfo::PackageType convertPackageType(const std::string &type) {
    if (type == "UNSUPPORTED") {
        return hippo::PackageInfo::UNSUPPORTED;
    } else if (type == "RPM") {
        return hippo::PackageInfo::RPM;
    } else if (type == "ARCHIVE") {
        return hippo::PackageInfo::ARCHIVE;
    }
    return hippo::PackageInfo::UNSUPPORTED;
}

template <>
void typedJsonize<hippo::PackageInfo>(
        hippo::PackageInfo &a,
        autil::legacy::Jsonizable::JsonWrapper &json)
{
    json.Jsonize("packageURI", a.URI, a.URI);
    if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
        std::string typeStr = convertPackageType(a.type);
        json.Jsonize("type", typeStr);
    } else {
        std::string typeStr;
        json.Jsonize("type", typeStr, "");
        a.type = convertPackageType(typeStr);
    }
};

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif //MASTER_FRAMEWORK_JSONIZABLEHELPER_H
