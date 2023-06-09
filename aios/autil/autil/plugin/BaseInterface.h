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
#include <memory>
#include "autil/Log.h"

namespace autil {

class BaseInterface {
public:
    BaseInterface() {};
    virtual ~BaseInterface() {};
    virtual const std::string interfaceName() const { return "builtin"; }
    virtual BaseInterface* create() { return nullptr; }
    virtual void destroy() { delete this; }
};

#define EXPORT_INTERFACE_CREATOR(prefix, name, classtype, classname)    \
    extern "C" classtype* prefix##name()                                \
    {                                                                   \
        return new classname();                                         \
    }

typedef std::shared_ptr<BaseInterface> BaseInterfacePtr;

} // namespace autil
