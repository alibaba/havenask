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
#include "iquan/jni/Jvm.h"

#include <string>

#include "fslib/fslib.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Utils.h"
#include "iquan/jni/ConstantJvmDefine.h"

// export from libhdfs.so
extern "C" {
JNIEnv *getJNIEnv(void);
}

namespace iquan {

JvmType Jvm::_jvmType = jt_invalid_jvm;

AUTIL_LOG_SETUP(iquan, Jvm);

Status Jvm::setup(JvmType jvmType, const std::vector<std::string> &classPaths, const std::string &jvmStartOpts) {
    if (unlikely(_jvmType != jt_invalid_jvm)) {
        return Status(IQUAN_INVALID_PARAMS, "Jvm::setup() has done before");
    }

    if (likely(jvmType == jt_hdfs_jvm)) {
        return hdfsSetup(classPaths, jvmStartOpts);
    } else {
        return Status(IQUAN_INVALID_JVM_TYPE, "invalid type of jvm");
    }
}

JNIEnv *Jvm::env() noexcept {
    if (likely(_jvmType == jt_hdfs_jvm)) {
        return getJNIEnv();
    }

    return NULL;
}

JNIEnv *Jvm::checkedEnv() {
    JNIEnv *env = Jvm::env();
    if (unlikely(!env)) {
        throw IquanException("JNIEnv is null.");
    }
    return env;
}

Status Jvm::hdfsSetup(const std::vector<std::string> &classPaths, const std::string &jvmStartOpts) {
    std::string oldHdfsOpts = Utils::getEnv(HDFSOPTS_KEY, "");
    AUTIL_LOG(INFO, "get original JVM env, key[%s], value[%s]", HDFSOPTS_KEY, oldHdfsOpts.c_str());

    std::string currentHdfsOpts = oldHdfsOpts;
    if (currentHdfsOpts.empty()) {
        currentHdfsOpts = JVM_START_OPTS;
    }

    std::string debugJvmFlags = Utils::getEnv(START_JVM_DEBUG, "");
    if (unlikely(!debugJvmFlags.empty())) {
        std::string debugJvmPort = Utils::getEnv(JVM_DEBUG_PORT, "");
        if (debugJvmPort.empty()) {
            debugJvmPort = JVM_DEFAULT_DEBUG_PORT;
        }

        if (currentHdfsOpts.find(JVM_DEBUG_OPTS_PATTERN) == std::string::npos) {
            currentHdfsOpts += std::string(JVM_DEBUG_OPTS) + debugJvmPort + " ";
        }
    }

    if (currentHdfsOpts != oldHdfsOpts) {
        Status status = Utils::setEnv(HDFSOPTS_KEY, currentHdfsOpts, true);
        if (!status.ok()) {
            std::string msg =
                std::string("failed to set JVM env, key [") + HDFSOPTS_KEY + "], value [" + currentHdfsOpts + "]";
            AUTIL_LOG(ERROR, "%s", msg.c_str());
            return Status(IQUAN_FAIL_TO_SET_ENV, msg);
        }
        AUTIL_LOG(INFO, "set JVM env success, key[%s], value[%s]", HDFSOPTS_KEY, currentHdfsOpts.c_str());
    }

    fslib::fs::AbstractFileSystem *fs = NULL;
    fslib::ErrorCode ec = fslib::fs::FileSystem::parseInternal(HDFS_PATH_FOR_INIT, fs);
    if (unlikely(fslib::EC_OK != ec) || unlikely(NULL == fs)) {
        std::string msg = "failed to init fslib with hdfs path";
        AUTIL_LOG(ERROR, "%s", msg.c_str());
        return Status(IQUAN_FAILED_TO_CALL_FSLIB, msg);
    }

    _jvmType = jt_hdfs_jvm;
    JNIEnv *env = Jvm::env();
    if (unlikely(env == NULL)) {
        _jvmType = jt_invalid_jvm;
        return Status(IQUAN_JNI_EXCEPTION, "JNIEnv is null");
    }

    jint version = env->GetVersion();
    if (unlikely(version < JNI_VERSION_1_8)) {
        _jvmType = jt_invalid_jvm;
        std::string msg = "expect jdk 1.8 or higer, current version is " + std::to_string(version);
        AUTIL_LOG(ERROR, "%s", msg.c_str());
        return Status(IQUAN_INVALID_JVM_VERSION, msg);
    }
    return Status::OK();
}

} // namespace iquan
