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

#include <map>
#include <set>
#include <stdint.h>
#include <string>

#include "autil/HashFunctionBase.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/common/TableDistribution.h"

namespace multi_call {
class RandomGenerator;
} // namespace multi_call

namespace navi {
class GigClientR;
class GraphBuilder;
class GraphDef;
class SubGraphDef;
}; // namespace navi

namespace sql {
class HashFunctionCacheR;

class PartAccessAssigner {
public:
    PartAccessAssigner(navi::GigClientR *gigClientR, HashFunctionCacheR *hashFunctionR = nullptr);
    ~PartAccessAssigner() = default;
    PartAccessAssigner(const PartAccessAssigner &) = delete;
    PartAccessAssigner &operator=(const PartAccessAssigner &) = delete;

public:
    bool process(navi::GraphDef *def);
    bool processSubGraph(navi::SubGraphDef *subGraphDef);

public:
    std::set<int32_t> genAccessPart(int32_t partCnt, const TableDistribution &tableDist);

private:
    autil::HashFunctionBasePtr createHashFunc(const HashMode &hashMode);
    std::set<int32_t> genDebugAccessPart(int32_t partCnt, const TableDistribution &tableDist);
    static std::set<int32_t> genAllPart(int32_t partCnt);
    static std::set<int32_t> getPartIds(const autil::RangeVec &ranges,
                                        const autil::RangeVec &hashIds);
    static void getPartId(const autil::RangeVec &ranges,
                          const autil::PartitionRange &hashId,
                          std::set<int32_t> &partIds);
    static uint64_t calHashKey(const std::string &hashName,
                               const std::map<std::string, std::string> &hashParams);

private:
    bool convertTableDistToPartIds(const std::string &bizName,
                                   const TableDistribution &tableDist,
                                   navi::GraphBuilder &builder);

private:
    navi::GigClientR *_gigClientR = nullptr;
    HashFunctionCacheR *_hashFunctionCacheR = nullptr;
    static multi_call::RandomGenerator _randomGenerator;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
