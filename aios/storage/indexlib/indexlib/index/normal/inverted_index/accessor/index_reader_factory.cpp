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
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"

#include <sstream>

#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_index_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_reader.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader.h"
#include "indexlib/index/normal/trie/trie_index_reader.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;

using namespace indexlib::common;

namespace indexlib { namespace index {

InvertedIndexReader* IndexReaderFactory::CreateIndexReader(InvertedIndexType indexType, IndexMetrics* indexMetrics)
{
    InvertedIndexReader* indexReader = NULL;
    switch (indexType) {
    case it_string:
    case it_text:
    case it_pack:
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
    case it_expack:
        return new NormalIndexReader(indexMetrics);
    case it_primarykey64:
    case it_primarykey128:
    case it_trie:
        return CreatePrimaryKeyIndexReader(indexType);
    case it_datetime:
        return new legacy::DateIndexReader();
    case it_range:
        return new legacy::RangeIndexReader();
    case it_spatial:
        return new legacy::SpatialIndexReader();
    case it_customized:
        return new CustomizedIndexReader();
    default:
        break;
    }
    stringstream s;
    s << "Index type " << indexType << " not implemented yet!";
    INDEXLIB_THROW(util::UnImplementException, "%s", s.str().c_str());
    return indexReader;
}

PrimaryKeyIndexReader* IndexReaderFactory::CreatePrimaryKeyIndexReader(InvertedIndexType indexType)
{
    if (indexType == it_primarykey64) {
        return new LegacyPrimaryKeyReader<uint64_t>();
    }
    if (indexType == it_primarykey128) {
        return new LegacyPrimaryKeyReader<autil::uint128_t>();
    }
    if (indexType == it_trie) {
        return new TrieIndexReader();
    }
    return NULL;
}
}} // namespace indexlib::index
