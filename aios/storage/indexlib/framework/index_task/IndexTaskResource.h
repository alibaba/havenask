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
#include <memory>
#include <string>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/BasicDefs.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::framework {

class IndexTaskResource : private autil::NoCopyable
{
public:
    IndexTaskResource(std::string name, IndexTaskResourceType type) : _name(std::move(name)), _type(std::move(type)) {}
    virtual ~IndexTaskResource() {}

public:
    const std::string& GetName() const { return _name; }
    const IndexTaskResourceType& GetType() const { return _type; }

public:
    virtual Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) = 0;
    virtual Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) = 0;

protected:
    std::string _name;
    IndexTaskResourceType _type;
};

} // namespace indexlibv2::framework
