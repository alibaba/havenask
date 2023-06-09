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

#include <stdint.h>
#include <map>
#include <string>

#include "ha3/common/Request.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class ConfigClause;
class VirtualAttributeClause;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

typedef std::map<std::string, std::string> ClauseOptionKVMap;

/*
  NOTE: this class is supposed to be used on qrs only
 */
class RequestCacheKeyGenerator
{
public:
    RequestCacheKeyGenerator(const RequestPtr &requestPtr);
    ~RequestCacheKeyGenerator();
private:
    RequestCacheKeyGenerator(const RequestCacheKeyGenerator &);
    RequestCacheKeyGenerator& operator=(const RequestCacheKeyGenerator &);
public:
    uint64_t generateRequestCacheKey() const;
    std::string generateRequestCacheString() const;
public:
    void disableClause(const std::string &clauseName);
    /*
      Now, we only support options in ConfigClause, KVPairClause 
      and VirtualAttributeClause
     */
    void disableOption(const std::string &clauseName, 
                       const std::string &optionName);
    bool setClauseOption(const std::string &clauseName, 
                         const std::string &optionName,
                         const std::string &optionValue);
    std::string getClauseOption(const std::string &clauseName, 
                                const std::string &optionName) const;
private:
    void convertConfigClauseToKVMap(const ConfigClause *configClause);
    void convertKVClauseToKVMap(const ConfigClause *configClause);
    void convertVirtualAttributeClauseToKVMap(
            const VirtualAttributeClause *vaClause);
private:
    std::map<std::string, ClauseOptionKVMap> _clauseKVMaps;
    std::map<std::string, std::string> _clauseCacheStrs;
private:
    friend class RequestCacheKeyGeneratorTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RequestCacheKeyGenerator> RequestCacheKeyGeneratorPtr;

} // namespace common
} // namespace isearch

