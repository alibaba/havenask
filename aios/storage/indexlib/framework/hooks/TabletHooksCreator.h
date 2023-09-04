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

#include <mutex>

#include "autil/Log.h"
#include "indexlib/util/Singleton.h"

namespace indexlib::framework {
class ITabletDeployerHook;

class TabletHooksCreator : public indexlib::util::Singleton<TabletHooksCreator>
{
public:
    TabletHooksCreator();
    ~TabletHooksCreator();

public:
    void RegisterTabletDeployerHook(const std::string& tableType, const std::unique_ptr<ITabletDeployerHook> hook);
    std::unique_ptr<ITabletDeployerHook> CreateTabletDeployerHook(std::string tableType) const;

private:
    mutable std::mutex _mutex;
    std::map<std::string, std::unique_ptr<ITabletDeployerHook>> _tabletDeployerHooks;
};

/*
   添加一种新的Hook，需要完成以下2个步骤:
   1.在hook cpp内调用 REGISTER_TABLET_DEPLOYER_HOOK(table_type, ClassName);
   2.在hook cpp对应的BUILD目标上添加 alwayslink = True
*/
#define REGISTER_TABLET_DEPLOYER_HOOK(TABLE_TYPE, TABLET_DEPLOYER_HOOK)                                                \
    __attribute__((constructor)) static void Register##TABLE_TYPE##TabletDeployerHook()                                \
    {                                                                                                                  \
        framework::TabletHooksCreator::GetInstance()->RegisterTabletDeployerHook(                                      \
            #TABLE_TYPE, std::make_unique<TABLET_DEPLOYER_HOOK>());                                                    \
    }

} // namespace indexlib::framework
