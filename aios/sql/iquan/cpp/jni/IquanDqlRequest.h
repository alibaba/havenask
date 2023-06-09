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

#include <string>
#include <unordered_map>

#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/common/IquanException.h"
#include "iquan/jni/DynamicParams.h"

namespace iquan {

class IquanDqlRequest : public autil::legacy::Jsonizable {
public:
    IquanDqlRequest();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool isValid() const;
    Status toCacheKey(std::string& jsonStr);
    Status toWarmupJson(std::string& jsonStr);
    Status fromWarmupJson(const std::string& jsonStr);
private:
    void defaultConfig();
    void cacheKeyConfig();
    void warmupJsonConfig();
public: // used for java
    std::vector<std::string> sqls;
    autil::legacy::json::JsonMap sqlParams;
    DynamicParams dynamicParams;
    std::string defaultCatalogName;
    std::string defaultDatabaseName;
public: // not use in java
    std::map<std::string, std::string> execParams; // used for exec graph
    size_t queryHashKey;
private: // for internal use
    bool _enableDynamicParams;
    bool _enableQueryHashKey;
};

typedef std::shared_ptr<IquanDqlRequest> IquanDqlRequestPtr;
} // namespace iquan
