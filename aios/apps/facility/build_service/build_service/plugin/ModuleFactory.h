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
#ifndef ISEARCH_BS_MODULEFACTORY_H
#define ISEARCH_BS_MODULEFACTORY_H

#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"
#include "indexlib/plugin/ModuleFactory.h"

namespace build_service { namespace plugin {

class ModuleFactory : public indexlib::plugin::ModuleFactory
{
public:
    ModuleFactory() {}
    virtual ~ModuleFactory() {}

public:
    using indexlib::plugin::ModuleFactory::init;
    virtual bool init(const KeyValueMap& parameters) { return true; }
    virtual bool init(const KeyValueMap& parameters, const config::ResourceReaderPtr& resourceReaderPtr)
    {
        return init(parameters);
    }

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::plugin

// for compatible
namespace build_service { namespace util {
using plugin::ModuleFactory;
}} // namespace build_service::util

#endif // ISEARCH_BS_MODULEFACTORY_H
