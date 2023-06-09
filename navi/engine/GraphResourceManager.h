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
#ifndef NAVI_GRAPHRESOURCEMANAGER_H
#define NAVI_GRAPHRESOURCEMANAGER_H

#include "navi/engine/ResourceMap.h"
#include "navi/proto/GraphDef.pb.h"

namespace navi {

class GraphResourceManager
{
private:
    typedef std::unordered_map<std::string, std::unordered_map<NaviPartId, ResourceMapPtr>> BizResourceMap;
public:
    GraphResourceManager();
    ~GraphResourceManager();
private:
    GraphResourceManager(const GraphResourceManager &);
    GraphResourceManager &operator=(const GraphResourceManager &);
public:
    ResourceMapPtr getBizResourceMap(const LocationDef &location);
private:
    ResourceMapPtr doGetMap(const std::string &bizName,
                            NaviPartId partId) const;
public:
    autil::ReadWriteLock _mapLock;
    BizResourceMap _bizResourceMap;
};

NAVI_TYPEDEF_PTR(GraphResourceManager);

}

#endif //NAVI_GRAPHRESOURCEMANAGER_H
