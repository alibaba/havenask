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
#ifndef NAVICONFIG_CONFIG_H
#define NAVICONFIG_CONFIG_H

#include "navi/config_loader/Config.h"
#include "autil/legacy/jsonizable.h"

namespace navi {

class Config
{
public:
    static bool load(const std::string &naviPythonPath,
                     const std::string &configLoader,
                     const std::string &configPath,
                     const std::string &loadParam,
                     std::string &jsonConfig);
};

}

#endif //NAVICONFIG_CONFIG_H
