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
#include "iquan/jni/wrapper/JClassLoader.h"

namespace iquan {

LocalRef<JClassLoader> JClassLoader::getSystemClassLoader() {
    static auto method = javaClass()->getStaticMethod<JClassLoader(void)>("getSystemClassLoader");
    return method(javaClass());
}

LocalRef<JClassLoader> JClassLoader::getExtClassLoader() {
    LocalRef<JClassLoader> current = JClassLoader::getSystemClassLoader();

    while (true) {
        LocalRef<JClassLoader> parent = current->getParent();
        if (parent.get() == NULL) {
            break;
        }
        current = parent;
    }
    return current;
}

LocalRef<JClassLoader> JClassLoader::getParent() {
    static auto method = javaClass()->getMethod<JClassLoader()>("getParent");
    return method(self());
}

LocalRef<jclass> JClassLoader::loadClass(const std::string &name) {
    static auto method = javaClass()->getMethod<jclass(std::string)>("loadClass");
    return method(self(), name);
}

} // namespace iquan
