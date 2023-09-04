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
#include "aios/network/opentelemetry/core/TraceUtil.h"

#include <iostream>
#include <pwd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include "alog/Configurator.h"
#include "alog/Logger.h"
#include "autil/StringConvertor.h"
#include "autil/StringUtil.h"

namespace opentelemetry {

std::string TraceUtil::joinMap(const std::map<std::string, std::string> &m, char first, char second) { return ""; }

} // namespace opentelemetry
