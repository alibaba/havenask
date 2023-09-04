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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "iquan/common/Common.h"
#include "iquan/jni/jnipp/jnipp.h"

namespace iquan {

class JIquanClient : public JavaClass<JIquanClient> {
public:
    static constexpr const char *kJavaDescriptor = "Lcom.taobao.search.iquan.client.IquanClient;";

    LocalRef<jbyteArray> updateCatalog(jint format, AliasRef<jbyteArray> request);
    LocalRef<jbyteArray>
    query(jint inputFormat, jint outputFormat, AliasRef<jbyteArray> rquest) const;

    LocalRef<jbyteArray> updateTables(jint format, AliasRef<jbyteArray> request);
    LocalRef<jbyteArray> updateLayerTables(jint format, AliasRef<jbyteArray> request);
    LocalRef<jbyteArray> updateFunctions(jint format, AliasRef<jbyteArray> request);

    // inspect interface
    LocalRef<jbyteArray> listCatalogs();
    LocalRef<jbyteArray> listDatabases(jint format, AliasRef<jbyteArray> request);
    LocalRef<jbyteArray> listTables(jint format, AliasRef<jbyteArray> request);
    LocalRef<jbyteArray> listFunctions(jint format, AliasRef<jbyteArray> request);
    LocalRef<jbyteArray> getTableDetails(jint format, AliasRef<jbyteArray> request);
    LocalRef<jbyteArray> getFunctionDetails(jint format, AliasRef<jbyteArray> request);
    LocalRef<jbyteArray> dumpCatalog();

public:
    static AliasRef<jclass> getSelfClazz();

private:
    static GlobalRef<jclass> _clazz;
    static autil::ThreadMutex _mutex;
    AUTIL_LOG_DECLARE();
};

} // namespace iquan
