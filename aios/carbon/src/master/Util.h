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
#ifndef CARBON_UTIL_H
#define CARBON_UTIL_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/RolePlan.h"
#include "carbon/GroupPlan.h"
#include "autil/legacy/jsonizable.h"

BEGIN_CARBON_NAMESPACE(master);

class Util
{
public:
    Util();
    ~Util();
private:
    Util(const Util &);
    Util& operator=(const Util &);
public:

    template<typename localT, typename targetT, typename localIt, typename targetIt>
    static void diff(const std::map<std::string, localT> &locals,
                     const std::map<std::string, targetT> &targets,
                     std::vector<std::string> &needAdds,
                     std::vector<std::string> &neeDels,
                     std::vector<std::string> &needUpdates);

    template<typename T>
    static bool getValue(const autil::legacy::json::JsonMap &jsonMap,
                         const std::string &key, T *value);
    
    static std::string getValue(const KVMap &kvMap,
                                const std::string &key,
                                const std::string &defaultValue = "");

    static int32_t calcGroupLatestVersionRatio(const GroupPlan &globalPlan);
    template<typename T>
    static void deepCopyPtr(const T &from, T *to);

    static std::string workerNodeIdToRoleGUID(const std::string &workerNodeId);
private:
    CARBON_LOG_DECLARE();
};

template<typename T>
inline bool Util::getValue(const autil::legacy::json::JsonMap &jsonMap,
                           const std::string &key, T *value)
{
    autil::legacy::json::JsonMap::const_iterator it = jsonMap.find(key);
    if (it == jsonMap.end()) {
        return false;
    }
    try {
        *value = autil::legacy::AnyCast<T>(it->second);
        return true;
    } catch (const autil::legacy::ExceptionBase &e) {
        return false;
    }
}

template<>
inline bool Util::getValue(const autil::legacy::json::JsonMap &jsonMap,
                           const std::string &key, int32_t *value)
{
    autil::legacy::json::JsonNumber jsonNum("");
    if (!getValue(jsonMap, key, &jsonNum)) {
        return false;
    }

    try {
        *value = jsonNum.Cast<int32_t>();
        return true;
    } catch (const autil::legacy::ExceptionBase &e) {
        return false;
    }
}


template<typename localT, typename targetT, typename localIT, typename targetIT>
void Util::diff(const std::map<std::string, localT> &locals,
                const std::map<std::string, targetT> &targets,
                std::vector<std::string> &needAdds,
                std::vector<std::string> &needDels,
                std::vector<std::string> &needUpdates)
{
    needAdds.clear();
    needDels.clear();
    needUpdates.clear();
    
    localIT localIt = locals.begin();
    targetIT targetIt = targets.begin();
    for (; localIt != locals.end() && targetIt != targets.end(); ) {
        if (localIt->first < targetIt->first) {
            needDels.push_back(localIt->first);
            localIt++;
        } else if (localIt->first > targetIt->first) {
            needAdds.push_back(targetIt->first);
            targetIt++;
        } else {
            needUpdates.push_back(localIt->first);
            localIt++;
            targetIt++;
        }
    }
    for (; localIt != locals.end(); localIt++) {
        needDels.push_back(localIt->first);
    }
    for (; targetIt != targets.end(); targetIt++) {
        needAdds.push_back(targetIt->first);
    }
}

CARBON_TYPEDEF_PTR(Util);

END_CARBON_NAMESPACE(master);

#endif //CARBON_UTIL_H
