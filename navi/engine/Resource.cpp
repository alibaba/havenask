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
#include "navi/log/NaviLogger.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceMap.h"

namespace navi {

Resource::Resource() {
}

Resource::~Resource() {
}

void Resource::addDepend(const std::shared_ptr<Resource> &depend) {
    if (depend.get() == this) {
        NAVI_KERNEL_LOG(WARN, "resource [%s] depend itself, ignored",
                        _def->resource_name().c_str());
        return;
    }
    _depends.push_back(depend);
}

void Resource::addDepend(const ResourceMap &resourceMap) {
    const auto &map = resourceMap.getMap();
    for (const auto &pair : map) {
        addDepend(pair.second);
    }
}

void Resource::setResourceDef(const std::shared_ptr<ResourceDef> &def) {
    _def = def;
}

const std::string &Resource::getName() {
    if (!_def) {
        auto resourceDef = new navi::ResourceDef();
        ResourceDefBuilder builder(resourceDef);
        def(builder);
        _def.reset(resourceDef);
    }
    return _def->resource_name();
}

void Resource::initLogger(NaviObjectLogger &logger, const std::string &prefix) {
    if (NAVI_TLS_LOGGER) {
        logger.object = this;
        logger.prefix = prefix;
        logger.logger = NAVI_TLS_LOGGER->logger;
    }
}

RootResource::RootResource(const std::string &name)
    : _name(name)
{
}

void RootResource::def(ResourceDefBuilder &builder) const {
    builder.name(_name);
}

}
