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
#ifndef __INDEXLIB_DOC_FILTER_CREATOR_H
#define __INDEXLIB_DOC_FILTER_CREATOR_H

#include <memory>

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/doc_filter.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class DocFilterCreator
{
public:
    DocFilterCreator() {}
    virtual ~DocFilterCreator() {}

public:
    virtual DocFilterPtr CreateDocFilter(const SegmentDirectoryPtr& segDir,
                                         const config::IndexPartitionSchemaPtr& schema,
                                         const config::IndexPartitionOptions& options) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocFilterCreator);
}} // namespace indexlib::merger

#endif //__INDEXLIB_DOC_FILTER_CREATOR_H
