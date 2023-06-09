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
#include "iquan/jni/IquanEnvImpl.h"

#include "iquan/common/Utils.h"
#include "iquan/jni/ConstantJvmDefine.h"
#include "iquan/jni/wrapper/JURL.h"
#include "iquan/jni/wrapper/JURLClassLoader.h"

namespace iquan {

AUTIL_LOG_SETUP(iquan, IquanEnvImpl);

GlobalRef<JClassLoader> IquanEnvImpl::_gIquanClassLoader = {nullptr};

Status IquanEnvImpl::load(const std::string &jarPath) {
    // 1. reset the global class loader
    _gIquanClassLoader.reset();

    // 2. if the current Iquan jar path is empty, we will use the binary path instad of jarPath
    std::string currentJarPath = jarPath;
    if (currentJarPath.empty()) {
        std::string binaryPath;
        IQUAN_ENSURE_FUNC(Utils::getBinaryPath(binaryPath));
        if (!binaryPath.empty()) {
            currentJarPath = binaryPath + IQUAN_CLIENT_JAR;
        } else {
            return Status(IQUAN_FAILED_TO_GET_BINARY_PATH, "binary path is empty");
        }
    }

    // 3. because the class path only accept the absolute the path,
    // so we need to transfer the path to the formal one
    std::string absClasspath;
    IQUAN_ENSURE_FUNC(Utils::getRealPath(currentJarPath, absClasspath));

    // 4. set the protocol for Java URL object
    absClasspath = "file://" + absClasspath;
    AUTIL_LOG(INFO, "abs jar class path is %s", absClasspath.c_str());

    // 5. initialize the url array
    LocalRef<JURL> url = JURL::newInstance(absClasspath);
    IQUAN_RETURN_ERROR_IF_NULL(url, IQUAN_NEW_INSTANCE_FAILED, "failed to create JURL");
    LocalRef<JObjectArrayT<JURL>> urlArray = JObjectArrayT<JURL>::newArray(1);
    urlArray->setElement(0, url);

    // 6. get the object iquan class loader
    LocalRef<JClassLoader> extClassLoader = JClassLoader::getExtClassLoader();
    IQUAN_RETURN_ERROR_IF_NULL(extClassLoader, IQUAN_NEW_INSTANCE_FAILED, "failed to get extClassLoader");

    _gIquanClassLoader = JURLClassLoader::newInstance(urlArray, extClassLoader);
    IQUAN_RETURN_ERROR_IF_NULL(
        _gIquanClassLoader, IQUAN_NEW_INSTANCE_FAILED, "failed to create global IquanClassLoader");

    // 7. return ok if success
    return Status::OK();
}

AliasRef<JClassLoader> IquanEnvImpl::getIquanClassLoader() { return _gIquanClassLoader; }

} // namespace iquan
