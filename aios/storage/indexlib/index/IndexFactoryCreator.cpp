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
#include "indexlib/index/IndexFactoryCreator.h"

#include <iosfwd>
#include <type_traits>
#include <typeinfo>

#include "indexlib/index/IIndexFactory.h"

using namespace std;

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, IndexFactoryCreator);

IndexFactoryCreator::IndexFactoryCreator() {}

IndexFactoryCreator::~IndexFactoryCreator() { Clean(); }

void IndexFactoryCreator::Clean()
{
    autil::ScopedLock lock(_mutex);
    for (auto iter : _factorysMap) {
        delete iter.second.second;
        iter.second.second = nullptr;
    }
    _factorysMap.clear();
}

void IndexFactoryCreator::Register(const string& indexType, IIndexFactory* factory)
{
    autil::ScopedLock lock(_mutex);
    auto iter = _factorysMap.find(indexType);
    if (iter != _factorysMap.end()) {
        IIndexFactory* oldFactory = iter->second.second;
        if (typeid(*oldFactory) == typeid(*factory)) {
            delete factory;
            return;
        }
        _factorysMap[indexType] =
            make_pair(Status::Corruption("duplicated index type %s, factory: %s and %s", indexType.c_str(),
                                         typeid(*oldFactory).name(), typeid(*factory).name()),
                      factory);
        delete oldFactory;
        return;
    }
    _factorysMap[indexType] = make_pair(Status::OK(), factory);
}

std::pair<Status, IIndexFactory*> IndexFactoryCreator::Create(const string& indexType) const
{
    autil::ScopedLock lock(_mutex);
    auto iter = _factorysMap.find(indexType);
    if (iter == _factorysMap.end()) {
        return make_pair(Status::NotFound(" no index type [%s]", indexType.c_str()), nullptr);
    }
    return iter->second;
}

std::unordered_map<autil::StringView, IIndexFactory*> IndexFactoryCreator::GetAllFactories() const
{
    std::unordered_map<autil::StringView, IIndexFactory*> allFactories;
    autil::ScopedLock lock(_mutex);
    for (const auto& [indexType, indexFactoryPair] : _factorysMap) {
        if (!indexFactoryPair.first.IsOK()) {
            AUTIL_LOG(ERROR, "index factory [%s] not ok, [%s]", indexType.c_str(),
                      indexFactoryPair.first.ToString().c_str());
        }
        allFactories[autil::StringView(indexType)] = indexFactoryPair.second;
    }
    return allFactories;
}
} // namespace indexlibv2::index
