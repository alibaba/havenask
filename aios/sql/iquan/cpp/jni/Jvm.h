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

#include <jni.h>
#include <memory>

#include "autil/Log.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/jni/JvmType.h"

namespace iquan {

class Jvm {
public:
    Jvm() = delete;
    ~Jvm() = delete;

    static Status setup(JvmType jvmType, const std::vector<std::string> &classPaths, const std::string &jvmStartOpts);
    static JNIEnv *env() noexcept;
    static JNIEnv *checkedEnv();

private:
    static Status hdfsSetup(const std::vector<std::string> &classPaths, const std::string &jvmStartOpts);

private:
    static JvmType _jvmType;
    AUTIL_LOG_DECLARE();
};

} // namespace iquan
