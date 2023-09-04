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
#include "navi/config_loader/Config.h"
#include "navi/config_loader/PythonEngine.h"

namespace navi {

bool Config::load(const std::string &naviPythonPath,
                  const std::string &configLoader,
                  const std::string &configPath,
                  const std::string &loadParam,
                  std::string &jsonConfig)
{
    return PythonEngine(naviPythonPath)
        .exec(configLoader, configPath, loadParam, jsonConfig);
}

}
