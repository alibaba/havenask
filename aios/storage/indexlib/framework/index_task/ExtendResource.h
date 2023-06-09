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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"

namespace indexlibv2::framework {

const IndexTaskResourceType ExtendResourceType = "ExtendResource";
template <typename T>
class ExtendResource : public IndexTaskResource
{
public:
    ExtendResource(const std::string& name, const std::shared_ptr<T>& resource)
        : IndexTaskResource(name, ExtendResourceType)
        , _resource(resource)
    {
    }
    ~ExtendResource() {}

    std::shared_ptr<T> GetResource() { return _resource; }

    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override
    {
        return Status::Corruption("not support yet");
    }
    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override
    {
        return Status::Corruption("not support yet");
    }

public:
    std::shared_ptr<T> _resource;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
