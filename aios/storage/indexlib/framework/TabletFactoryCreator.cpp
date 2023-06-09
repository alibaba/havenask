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
#include "indexlib/framework/TabletFactoryCreator.h"

#include "indexlib/framework/ITabletFactory.h"

using namespace std;

namespace indexlibv2::framework {
class ITabletFactory;

TabletFactoryCreator::TabletFactoryCreator() {}

TabletFactoryCreator::~TabletFactoryCreator()
{
    autil::ScopedLock lock(_mutex);
    for (auto iter : _factorysMap) {
        delete iter.second;
        iter.second = nullptr;
    }
}

std::unique_ptr<ITabletFactory> TabletFactoryCreator::Create(std::string tableType) const
{
    autil::ScopedLock lock(_mutex);
    auto iter = _factorysMap.find(tableType);
    if (iter == _factorysMap.end()) {
        return nullptr;
    }
    return iter->second->Create();
}

void TabletFactoryCreator::Register(std::string tableType, ITabletFactory* factory)
{
    autil::ScopedLock lock(_mutex);
    auto iter = _factorysMap.find(tableType);
    if (iter != _factorysMap.end()) {
        std::abort();
    }
    _factorysMap[tableType] = factory;
}

std::vector<std::string> TabletFactoryCreator::GetRegisteredType() const
{
    autil::ScopedLock lock(_mutex);
    std::vector<std::string> types;
    for (auto iter : _factorysMap) {
        types.push_back(iter.first);
    }
    return types;
}

} // namespace indexlibv2::framework
