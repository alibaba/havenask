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

#include <string>

#include "autil/SharedObjectMap.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/log/NaviLogger.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class SharedObjectMapR : public navi::Resource {
public:
    SharedObjectMapR();
    ~SharedObjectMapR();
    SharedObjectMapR(const SharedObjectMapR &) = delete;
    SharedObjectMapR &operator=(const SharedObjectMapR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    template <typename T>
    T *getObject(const std::string &name) const {
        T *object = nullptr;
        if (!_sharedObjectMap.get<T>(name, object)) {
            NAVI_KERNEL_LOG(WARN, "get shared object failed, name [%s]", name.c_str());
            return nullptr;
        }
        return object;
    }
    autil::SharedObjectMap *getSharedObjectMap() {
        return &_sharedObjectMap;
    }

public:
    static const std::string RESOURCE_ID;

private:
    autil::SharedObjectMap _sharedObjectMap;
};

NAVI_TYPEDEF_PTR(SharedObjectMapR);

} // namespace sql
