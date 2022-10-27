#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_reader.h"
#include "indexlib/index/normal/trie/trie_index_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_reader.h"
#include "indexlib/misc/exception.h"
#include <sstream>

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(common);


IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);

IndexReader* IndexReaderFactory::CreateIndexReader(IndexType indexType)
{
    IndexReader *indexReader = NULL;
    switch (indexType)
    {
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
        return new NormalIndexReader();
    case it_primarykey64:
    case it_primarykey128:
    case it_trie:
        return CreatePrimaryKeyIndexReader(indexType);
    case it_date:
        return new DateIndexReader();
    case it_range:
        return new RangeIndexReader();
    case it_spatial:
        return new SpatialIndexReader();
    case it_customized:
        return new CustomizedIndexReader();
    default:
        break;
    }
    stringstream s;
    s << "Index type " << indexType <<" not implemented yet!";
    INDEXLIB_THROW(misc::UnImplementException, "%s", s.str().c_str());
    return indexReader;
}

PrimaryKeyIndexReader* IndexReaderFactory::CreatePrimaryKeyIndexReader(
            IndexType indexType)
{
    if (indexType == it_primarykey64)
    {
        return new UInt64PrimaryKeyIndexReader();
    }
    if (indexType == it_primarykey128)
    {
        return new UInt128PrimaryKeyIndexReader();
    }
    if (indexType == it_trie)
    {
        return new TrieIndexReader();
    }
    return NULL;
}

IE_NAMESPACE_END(index);

