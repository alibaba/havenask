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

#include "navi/engine/Resource.h"
#include "navi/engine/ResourceMap.h"

namespace navi {

class MultiLayerResourceMap {
public:
    MultiLayerResourceMap() {
    }
public:
    void addResourceMap(const ResourceMap *resourceMap);
    std::shared_ptr<Resource> getResource(const std::string &name) const;
    bool hasResource(const std::string &name) const;
    bool hasResource(const ResourcePtr &resource) const;
    void append(const MultiLayerResourceMap &other);
public:
    friend std::ostream& operator<<(std::ostream& os,
                                    const MultiLayerResourceMap &multiLayerResourceMap);
private:
    std::vector<const ResourceMap *> _resourceMapVec;
};

}

