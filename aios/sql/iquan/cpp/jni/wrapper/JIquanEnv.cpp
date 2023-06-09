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
#include "iquan/jni/wrapper/JIquanEnv.h"

#include "iquan/jni/IquanEnvImpl.h"

namespace iquan {

AUTIL_LOG_SETUP(iquan, JIquanEnv);

GlobalRef<jclass> JIquanEnv::_clazz = {nullptr};
autil::ThreadMutex JIquanEnv::_mutex;

LocalRef<jbyteArray> JIquanEnv::initEnvResource() {
    AliasRef<jclass> clazz = JIquanEnv::getSelfClazz();
    if (clazz.get() == nullptr) {
        AUTIL_LOG(ERROR, "JIquanEnv class is null");
        return nullptr;
    }
    auto method = clazz->getStaticMethod<jbyteArray()>("initEnvResource");
    return method(clazz.get());
}

LocalRef<jbyteArray> JIquanEnv::startKmon(jint format, AliasRef<jbyteArray> params) {
    AliasRef<jclass> clazz = JIquanEnv::getSelfClazz();
    if (clazz.get() == nullptr) {
        AUTIL_LOG(ERROR, "JIquanEnv class is null");
        return nullptr;
    }
    auto method = clazz->getStaticMethod<jbyteArray(jint, jbyteArray)>("startKmon");
    return method(clazz.get(), format, params);
}

LocalRef<jbyteArray> JIquanEnv::stopKmon() {
    AliasRef<jclass> clazz = JIquanEnv::getSelfClazz();
    if (clazz.get() == nullptr) {
        AUTIL_LOG(ERROR, "JIquanEnv class is null");
        return nullptr;
    }
    auto method = clazz->getStaticMethod<jbyteArray()>("stopKmon");
    return method(clazz.get());
}

AliasRef<jclass> JIquanEnv::getSelfClazz() {
    autil::ScopedLock scopedLock(_mutex);
    if (_clazz.get() != nullptr) {
        return _clazz;
    }

    AliasRef<JClassLoader> classLoader = IquanEnvImpl::getIquanClassLoader();
    if (classLoader.get() == nullptr) {
        AUTIL_LOG(ERROR, "global iquan ClassLoader is null");
        return nullptr;
    }

    LocalRef<jclass> localClazz = classLoader->loadClass("com.taobao.search.iquan.client.IquanEnv");
    _clazz = localClazz;
    return _clazz;
}

} // namespace iquan
