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
#include <stddef.h>
#include <string>

#include "autil/Log.h"

namespace autil {

class BaseInterface;

class InterfaceManager {
public:
    InterfaceManager(){};
    virtual ~InterfaceManager();
    InterfaceManager(const InterfaceManager &) = delete;
    InterfaceManager &operator=(const InterfaceManager &) = delete;

public:
    bool addInterface(const std::string &name, BaseInterface *interface);
    BaseInterface *find(const std::string &name) const;
    void clear();
    size_t size() const { return _interfaceMap.size(); }

    template <class T>
    T *get(const std::string &name) const {
        auto interface = this->find(name);
        if (interface) {
            return dynamic_cast<T *>(interface);
        }
        return nullptr;
    }

private:
    std::map<const std::string, BaseInterface *> _interfaceMap;

private:
    AUTIL_LOG_DECLARE();
};

#define CREATE_INTERFACE_INSTANCE(type) new type()
#define REGISTER_INTERFACE(name, interface, manager)                                                                   \
    do {                                                                                                               \
        auto instance = CREATE_INTERFACE_INSTANCE(interface);                                                          \
        if (instance) {                                                                                                \
            if (!manager->addInterface(name, instance)) {                                                              \
                return false;                                                                                          \
            }                                                                                                          \
        } else {                                                                                                       \
            return false;                                                                                              \
        }                                                                                                              \
    } while (false)

} // namespace autil
