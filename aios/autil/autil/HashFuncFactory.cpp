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
#include "autil/HashFuncFactory.h"

#include <iosfwd>

#include "autil/DefaultHashFunction.h"
#include "autil/ExtendHashFunction.h"
#include "autil/Hash64Function.h"
#include "autil/KsHashFunction.h"
#include "autil/Log.h"
#include "autil/MurmurHash3Function.h"
#include "autil/NumberHashFunction.h"
#include "autil/RoutingHashFunction.h"
#include "autil/SmartWaveHashFunction.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

using namespace std;

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, HashFuncFactory);

const std::string SEP = "#";
HashFuncFactory::HashFuncFactory() {}

HashFuncFactory::~HashFuncFactory() {}

HashFunctionBasePtr HashFuncFactory::createHashFunc(const string &funcName, uint32_t partitionCount) {
    static map<string, string> params;
    return HashFuncFactory::createHashFunc(funcName, params, partitionCount);
}

HashFunctionBasePtr
HashFuncFactory::createHashFunc(const string &funcName, const map<string, string> &params, uint32_t partitionCount) {
    StringTokenizer tokenizer(funcName, SEP, StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (tokenizer.getNumTokens() <= 0) {
        return HashFunctionBasePtr();
    }
    string realFuncName = tokenizer[0];
    if (realFuncName == "HASH") {
        return HashFunctionBasePtr(new DefaultHashFunction(realFuncName, partitionCount));
    } else if (realFuncName == "HASH64") {
        return HashFunctionBasePtr(new Hash64Function(realFuncName, partitionCount));
    } else if (realFuncName == "GALAXY_HASH") {
        return HashFunctionBasePtr(new ExtendHashFunction(realFuncName, partitionCount));
    } else if (realFuncName == "NUMBER_HASH") {
        return HashFunctionBasePtr(new NumberHashFunction(realFuncName, partitionCount));
    } else if (realFuncName == "KINGSO_HASH") {
        uint32_t ksHashRange = 720;
        if (tokenizer.getNumTokens() == 2) {
            uint32_t value = 0;
            bool ret = StringUtil::fromString(tokenizer[1], value);
            if (ret && value != 0) {
                ksHashRange = value;
            } else {
                AUTIL_LOG(WARN, "parameter %s is invalid.", funcName.c_str());
            }
        }
        return HashFunctionBasePtr(new KsHashFunction(realFuncName, partitionCount, ksHashRange));
    } else if (realFuncName == "ROUTING_HASH") {
        HashFunctionBasePtr base(new RoutingHashFunction(realFuncName, params, partitionCount));
        if (!base->init()) {
            AUTIL_LOG(WARN, "init routing hash function failed");
            return HashFunctionBasePtr();
        } else {
            return base;
        }
    } else if (realFuncName == "SMARTWAVE_HASH") {
        HashFunctionBasePtr base(new SmartWaveHashFunction(realFuncName, params, partitionCount));
        if (!base->init()) {
            AUTIL_LOG(WARN, "init smartwave hash function failed");
            return HashFunctionBasePtr(new DefaultHashFunction(realFuncName, partitionCount));
        }
        return base;
    } else if (realFuncName == MurmurHash3Function::name()) {
        // MURMUR_HASH3
        HashFunctionBasePtr base(new MurmurHash3Function(realFuncName, params, partitionCount));
        if (!base->init()) {
            AUTIL_LOG(ERROR, "init murmur-hash3 function failed");
            return HashFunctionBasePtr();
        }
        return base;
    } else {
        return HashFunctionBasePtr();
    }
}

} // namespace autil
