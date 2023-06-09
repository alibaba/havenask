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
#include "iquan/jni/IquanEnv.h"

#include <thread>

#include "iquan/common/Common.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Utils.h"
#include "iquan/jni/ConstantJvmDefine.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "iquan/jni/IquanEnvImpl.h"
#include "iquan/jni/Jvm.h"
#include "iquan/jni/jnipp/Exceptions.h"
#include "iquan/jni/wrapper/JIquanEnv.h"

namespace iquan {

AUTIL_LOG_SETUP(iquan, IquanEnv);

static std::once_flag FLAG;
static Status STATUS;

static void doJvmSetup(JvmType jvmType, const std::vector<std::string> &classPaths, const std::string &jvmStartOps) {
    std::call_once(FLAG, [&]() {
        try {
            STATUS = Jvm::setup(jvmType, classPaths, jvmStartOps);
            if (!STATUS.ok()) {
                return;
            }

            STATUS = IquanEnvImpl::load("");
            if (!STATUS.ok()) {
                return;
            }

            LocalRef<JByteArray> responseByteArray = JIquanEnv::initEnvResource();
            if (likely(responseByteArray.get() != nullptr)) {
                std::string responseStr = JByteArrayToStdString(responseByteArray);
                STATUS = ResponseHeader::check(responseStr);
                if (!STATUS.ok()) {
                return;
            }
            } else {
                STATUS = Status(IQUAN_RESPONSE_IS_NULL, "IquanEnv::initEnvResource() response is null");
                return;
            }
        } catch (const JnippException &e) {
            STATUS = Status(IQUAN_JNI_EXCEPTION, e.what());
            return;
        } catch (const IquanException &e) {
            STATUS = Status(e.code(), e.what());
            return;
        } catch (const std::exception &e) {
            STATUS = Status(IQUAN_FAIL, e.what());
            return;
        }
        STATUS = Status::OK();
    });
}

Status IquanEnv::jvmSetup(JvmType jvmType, const std::vector<std::string> &classPaths, const std::string &jvmStartOps) {
    std::thread t(doJvmSetup, jvmType, classPaths, jvmStartOps);
    t.join();
    return STATUS;
}

Status IquanEnv::startKmon(const KMonConfig &kmonConfig) {
    if (!kmonConfig.isValid()) {
        return Status(IQUAN_INVALID_PARAMS, "kmonConfig is not valid");
    }

    try {
        std::string kmonConfigStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(kmonConfig, kmonConfigStr));

        LocalRef<JByteArray> kmonConfigByteArray = JByteArrayFromStdString(kmonConfigStr);
        IQUAN_RETURN_ERROR_IF_NULL(kmonConfigByteArray, IQUAN_FAIL, "failed to create byte array for kmonConfig");

        LocalRef<JByteArray> responseByteArray = JIquanEnv::startKmon(INPUT_JSON_FORMAT, kmonConfigByteArray);
        if (responseByteArray.get()) {
            std::string responseStr = JByteArrayToStdString(responseByteArray);
            IQUAN_ENSURE_FUNC(ResponseHeader::check(responseStr));
        } else {
            return Status(IQUAN_RESPONSE_IS_NULL, "IquanEnv::startKmon() response is null");
        }
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) { return Status(e.code(), e.what()); } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

Status IquanEnv::stopKmon() {
    try {
        LocalRef<JByteArray> responseByteArray = JIquanEnv::stopKmon();
        if (likely(responseByteArray.get() != nullptr)) {
            std::string responseStr = JByteArrayToStdString(responseByteArray);
            IQUAN_ENSURE_FUNC(ResponseHeader::check(responseStr));
        } else {
            return Status(IQUAN_RESPONSE_IS_NULL, "IquanEnv::stopKmon() response is null");
        }
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) { return Status(e.code(), e.what()); } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

} // namespace iquan
