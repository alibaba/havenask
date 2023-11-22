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

#include "indexlib/util/Singleton.h"

namespace indexlibv2::framework {
class CustomIndexTaskFactory;

class CustomIndexTaskFactoryCreator : public indexlib::util::Singleton<CustomIndexTaskFactoryCreator>
{
public:
    CustomIndexTaskFactoryCreator();
    ~CustomIndexTaskFactoryCreator();

public:
    void Register(std::string factoryName, CustomIndexTaskFactory* factory);
    std::unique_ptr<CustomIndexTaskFactory> Create(std::string factoryName) const;
    std::vector<std::string> GetRegisteredType() const;

private:
    std::map<std::string, CustomIndexTaskFactory*> _factorysMap;
    mutable autil::ThreadMutex _mutex;
};

} // namespace indexlibv2::framework
