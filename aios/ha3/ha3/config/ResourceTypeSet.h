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
#include <set>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace config {

typedef std::string ResourceType;

class ResourceTypeSet
{
public:
    ResourceTypeSet();
    ResourceTypeSet(const std::string &str) {
        fromString(str);
    }

    ~ResourceTypeSet();


    void fromString(const std::string &str);
    
    std::string toString() const;

    bool has(const ResourceType &resourceType) const {
        if (resourceType.empty() && empty()) {
            return true;
        }
        return _types.find(resourceType) != _types.end();
    }

    const std::set<ResourceType>& getTypes() const {
        return _types;
    }
    
    bool empty() const;
    
    ResourceTypeSet& operator = (const std::string &str) {
        fromString(str);
        return *this;
    }

private:
    std::set<ResourceType> _types;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ResourceTypeSet> ResourceTypeSetPtr;

} // namespace config
} // namespace isearch

