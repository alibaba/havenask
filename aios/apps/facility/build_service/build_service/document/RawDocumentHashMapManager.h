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
#ifndef ISEARCH_BS_RAWDOCUMENTHASHMAPMANAGER_H
#define ISEARCH_BS_RAWDOCUMENTHASHMAPMANAGER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/document/raw_document/key_map_manager.h"

namespace build_service { namespace document {

typedef indexlib::document::KeyMapManager RawDocumentHashMapManager;
BS_TYPEDEF_PTR(RawDocumentHashMapManager);

}} // namespace build_service::document

#endif // ISEARCH_BS_RAWDOCUMENTHASHMAPMANAGER_H
