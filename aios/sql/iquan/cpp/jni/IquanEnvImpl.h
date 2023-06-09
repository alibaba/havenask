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

#include "autil/Log.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/jni/jnipp/jnipp.h"
#include "iquan/jni/wrapper/JClassLoader.h"

namespace iquan {

class IquanEnvImpl {
public:
    static AliasRef<JClassLoader> getIquanClassLoader();
    static Status load(const std::string &jarPath = "");

private:
    static GlobalRef<JClassLoader> _gIquanClassLoader;
    AUTIL_LOG_DECLARE();
};

} // namespace iquan
