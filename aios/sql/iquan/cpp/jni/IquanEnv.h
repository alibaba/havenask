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

#include <mutex>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/config/KMonConfig.h"
#include "iquan/jni/JvmType.h"

namespace iquan {

class IquanEnv {
public:
    static Status jvmSetup(JvmType jvmType,
                           const std::vector<std::string> &classPaths,
                           const std::string &jvmStartOps);
    static Status startKmon(const KMonConfig &kmonConfig);
    static Status stopKmon();

private:
    AUTIL_LOG_DECLARE();
};

} // namespace iquan
