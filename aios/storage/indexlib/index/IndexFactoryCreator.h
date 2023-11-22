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
#include <unordered_map>
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/StringView.h"
#include "indexlib/base/Status.h"
#include "indexlib/util/Singleton.h"

namespace indexlibv2::index {

class IIndexFactory;

class IndexFactoryCreator : public indexlib::util::Singleton<IndexFactoryCreator>
{
public:
    IndexFactoryCreator();
    ~IndexFactoryCreator();

public:
    void Clean();
    void Register(const std::string& indexType, IIndexFactory* factory);
    std::pair<Status, IIndexFactory*> Create(const std::string& indexType) const;
    std::unordered_map<autil::StringView, IIndexFactory*> GetAllFactories() const;

private:
    std::map<std::string, std::pair<Status, IIndexFactory*>> _factorysMap;
    mutable autil::ThreadMutex _mutex;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
