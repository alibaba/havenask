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
#ifndef ISEARCH_BS_SWIFTROOTUPGRADER_H
#define ISEARCH_BS_SWIFTROOTUPGRADER_H

#include "build_service/common_define.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/util/Log.h"

namespace build_service { namespace proto {

// use for upgrade swift root in swift data source
// TODO: remove this after upgrade done
// 1. upgrade_swift_root_from = swift_root1#swift_root2
// 2. upgrade_swfit_root_to   = new_root1#new_root2
// means : swift_root1 -> new_root1 #  swift_root2 -> new_root2

/* FOR TEST:
 * upgrade_swift_root_target_generations = 12333456,62346234
 * only generation set in target needUpgrade
 * all generation will be needUpgrade if no generation id set
 */
class SwiftRootUpgrader
{
public:
    SwiftRootUpgrader();
    ~SwiftRootUpgrader();

private:
    SwiftRootUpgrader(const SwiftRootUpgrader&);
    SwiftRootUpgrader& operator=(const SwiftRootUpgrader&);

public:
    bool needUpgrade(uint32_t generationId) const;
    void upgrade(proto::DataDescription& ds);
    void upgrade(proto::DataDescriptions& dsVec);
    std::string upgradeDataDescriptions(const std::string& str);

private:
    std::vector<std::string> _swiftOrgVec;
    std::vector<std::string> _swiftNewVec;
    // for test : empty means all
    std::set<uint32_t> _targetGenerations;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftRootUpgrader);

}} // namespace build_service::proto

#endif // ISEARCH_BS_SWIFTROOTUPGRADER_H
