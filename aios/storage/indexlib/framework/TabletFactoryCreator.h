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
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/util/Singleton.h"

namespace indexlibv2::framework {

class ITabletFactory;

class TabletFactoryCreator : public indexlib::util::Singleton<TabletFactoryCreator>
{
public:
    TabletFactoryCreator();
    ~TabletFactoryCreator();

    void Register(std::string tableType, ITabletFactory* factory);
    std::unique_ptr<ITabletFactory> Create(std::string tableType) const;
    std::vector<std::string> GetRegisteredType() const;

private:
    std::map<std::string, ITabletFactory*> _factorysMap;
    mutable autil::ThreadMutex _mutex;
};

} // namespace indexlibv2::framework
