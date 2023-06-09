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
#include "iquan/jni/jnipp/Exceptions.h"

#include "iquan/jni/jnipp/CoreClasses.h"
#include "jni.h"

namespace iquan {

extern JNIEnv *getIquanJNIEnv();

void transJniException() {
    JNIEnv *env = getIquanJNIEnv();
    jthrowable exception = env->ExceptionOccurred();
    if (!exception) {
        return;
    }
    env->ExceptionClear();

    LocalRef<JThrowable> throwable(exception);
    std::string result = "[exception] " + throwable->toStdString() + "\n";
    LocalRef<JObjectArrayT<JStackTraceElement>> stackTraces = throwable->getStackTrace();
    for (size_t i = 0; i < stackTraces->size(); i++) {
        LocalRef<JStackTraceElement> stackTraceElement = stackTraces->getElement(i);
        result += stackTraceElement->toStdString() + "\n";
    }
    throw JnippException(result);
}

} // namespace iquan
