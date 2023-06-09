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
#ifndef __INDEXLIB_DOCUMENT_DEDUPER_HELPER_H
#define __INDEXLIB_DOCUMENT_DEDUPER_HELPER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class DocumentDeduperHelper
{
public:
    DocumentDeduperHelper();
    ~DocumentDeduperHelper();

public:
    static bool DelayDedupDocument(const IndexPartitionOptions& options, const IndexConfigPtr& pkConfig);
};

DEFINE_SHARED_PTR(DocumentDeduperHelper);
}} // namespace indexlib::config

#endif //__INDEXLIB_DOCUMENT_DEDUPER_HELPER_H
