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
#ifndef __INDEXLIB_INDEX_READER_FACTORY_H
#define __INDEXLIB_INDEX_READER_FACTORY_H

#include <map>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, InvertedIndexReader);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, IndexMetrics);

namespace indexlib { namespace index {

class IndexReaderFactory
{
public:
    static InvertedIndexReader* CreateIndexReader(InvertedIndexType indexType, IndexMetrics* indexMetrics);

    static PrimaryKeyIndexReader* CreatePrimaryKeyIndexReader(InvertedIndexType indexType);
};
}} // namespace indexlib::index

#endif //__INDEXLIB_INDEX_READER_FACTORY_H
