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
#include "iquan/jni/wrapper/JIquanClient.h"

#include "iquan/jni/IquanEnvImpl.h"
#include "iquan/jni/wrapper/JClassLoader.h"

namespace iquan {

AUTIL_LOG_SETUP(iquan, JIquanClient);

GlobalRef<jclass> JIquanClient::_clazz = {nullptr};
autil::ThreadMutex JIquanClient::_mutex;

LocalRef<jbyteArray> JIquanClient::updateTables(jint format, AliasRef<jbyteArray> request) {
    static auto method = getClass()->getMethod<jbyteArray(jint, jbyteArray)>("updateTables");
    return method(self(), format, request.get());
}

LocalRef<jbyteArray> JIquanClient::updateLayerTables(jint format, AliasRef<jbyteArray> request) {
    static auto method = getClass()->getMethod<jbyteArray(jint, jbyteArray)>("updateLayerTables");
    return method(self(), format, request.get());
}

LocalRef<jbyteArray> JIquanClient::updateCatalog(jint format, AliasRef<jbyteArray> request) {
    static auto method = getClass()->getMethod<jbyteArray(jint, jbyteArray)>("updateCatalog");
    return method(self(), format, request.get());
}

LocalRef<jbyteArray> JIquanClient::updateFunctions(jint format, AliasRef<jbyteArray> request) {
    static auto method = getClass()->getMethod<jbyteArray(jint, jbyteArray)>("updateFunctions");
    return method(self(), format, request.get());
}

LocalRef<jbyteArray>
JIquanClient::query(jint inputFormat, jint outputFormat, AliasRef<jbyteArray> request) const {
    static auto method = getClass()->getMethod<jbyteArray(jint, jint, jbyteArray)>("sqlQuery");
    return method(self(), inputFormat, outputFormat, request.get());
}

LocalRef<jbyteArray> JIquanClient::listCatalogs() {
    static auto method = getClass()->getMethod<jbyteArray(void)>("listCatalog");
    return method(self());
}

LocalRef<jbyteArray> JIquanClient::listDatabases(jint format, AliasRef<jbyteArray> request) {
    static auto method = getClass()->getMethod<jbyteArray(jint, jbyteArray)>("listDatabase");
    return method(self(), format, request.get());
}

LocalRef<jbyteArray> JIquanClient::listTables(jint format, AliasRef<jbyteArray> request) {
    static auto method = getClass()->getMethod<jbyteArray(jint, jbyteArray)>("listTables");
    return method(self(), format, request.get());
}

LocalRef<jbyteArray> JIquanClient::listFunctions(jint format, AliasRef<jbyteArray> request) {
    static auto method = getClass()->getMethod<jbyteArray(jint, jbyteArray)>("listFunctions");
    return method(self(), format, request.get());
}

LocalRef<jbyteArray> JIquanClient::getTableDetails(jint format, AliasRef<jbyteArray> request) {
    static auto method = getClass()->getMethod<jbyteArray(jint, jbyteArray)>("getTableDetails");
    return method(self(), format, request.get());
}

LocalRef<jbyteArray> JIquanClient::getFunctionDetails(jint format, AliasRef<jbyteArray> request) {
    static auto method = getClass()->getMethod<jbyteArray(jint, jbyteArray)>("getFunctionDetails");
    return method(self(), format, request.get());
}

LocalRef<jbyteArray> JIquanClient::dumpCatalog() {
    static auto method = getClass()->getMethod<jbyteArray(void)>("dumpCatalog");
    return method(self());
}

AliasRef<jclass> JIquanClient::getSelfClazz() {
    autil::ScopedLock scopedLock(_mutex);
    if (_clazz.get() != nullptr) {
        return _clazz;
    }

    AliasRef<JClassLoader> classLoader = IquanEnvImpl::getIquanClassLoader();
    if (classLoader.get() == nullptr) {
        AUTIL_LOG(ERROR, "urlClassLoader is null");
        return nullptr;
    }

    LocalRef<jclass> localClazz
        = classLoader->loadClass("com.taobao.search.iquan.client.IquanClient");
    _clazz = localClazz;
    return _clazz;
}

} // namespace iquan
