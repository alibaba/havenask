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
#include "master/Flag.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include <functional>

BEGIN_CARBON_NAMESPACE(master);
using namespace autil;

#define K_CARBON_SILENT_UNPUBLISH "CARBON_SILENT_UNPUBLISH"
#define K_CARBON_ROUTER_CONF "CARBON_ROUTER_CONF"
#define K_CARBON_ROUTER_All_K8S "CARBON_ROUTER_ALL_K8S"
#define K_CARBON_TEST_PUBLISH "CARBON_TEST_PUBLISH"
#define K_C2_DRIVER_PROXY "C2_DRIVER_PROXY"

typedef std::function<void* (const char* v)> Parser;
typedef std::map<const char*, Parser> Parsers;
static Parsers parsers = {
    {K_CARBON_SILENT_UNPUBLISH, [](const char* v) -> void* {
        int32_t t = 0;
        static int32_t ret = 0;
        ret = StringUtil::strToInt32(v, t) ? t : ret;
        return &ret;
    }},
    {K_CARBON_ROUTER_CONF, [](const char* v) -> void* {
        static RouterBaseConf conf;
        auto vec = autil::StringUtil::split(v, ",");
        if (vec.size() > 0) {
            conf.host = vec[0];
        }
        if (vec.size() > 1) {
            StringUtil::fromString(vec[1], conf.single);
        }
        if (vec.size() > 2) {
            StringUtil::fromString(vec[2], conf.useJsonEncode);
        }
        return &conf;
    }},
    {K_C2_DRIVER_PROXY, [](const char* v) -> void* {
        static std::string proxy = v;
        return &proxy;
    }},
    {K_CARBON_ROUTER_All_K8S, [](const char* v) -> void* {
        static bool ret = false;
        bool allK8s = false;
        if (strcmp(v, "true") == 0) {
            allK8s = true;
        } 
        ret = allK8s;
        return &ret;
    }},
    {K_CARBON_TEST_PUBLISH, [](const char* v) -> void* {
        static bool ret = false;
        bool testPublish = false;
        if (strcmp(v, "true") == 0) {
            testPublish = true;
        } 
        ret = testPublish;
        return &ret;
    }},
}; 

// pass lexically string here to compare the static variable address
static void* getAndParse(const char* k) {
    std::string v = autil::EnvUtil::getEnv(k);
    if (!v.empty()) {
        auto parser = parsers[k]; // compare address
        return parser(v.c_str());
    }
    return NULL;
}

bool Flag::isSilentUnpublish(int type) {
    void* t = getAndParse(K_CARBON_SILENT_UNPUBLISH);
    return t == NULL ? false : type == *((int32_t*) t);
}

RouterBaseConf Flag::getRouterConf() {
    auto confP =  (RouterBaseConf*)getAndParse(K_CARBON_ROUTER_CONF);
    return confP == NULL ? RouterBaseConf() : *confP;
}

bool Flag::getAllK8s() {
    void* t = getAndParse(K_CARBON_ROUTER_All_K8S);
    return t == NULL ? false :  *((bool*)t);
}

bool Flag::isTestPublish() {
    void* t = getAndParse(K_CARBON_TEST_PUBLISH);
    return t == NULL ? false :  *((bool*)t);
}

bool Flag::isSlotOnC2() {
    void* t = getAndParse(K_C2_DRIVER_PROXY);
    return t == NULL ? false : true;
}

END_CARBON_NAMESPACE(master);
