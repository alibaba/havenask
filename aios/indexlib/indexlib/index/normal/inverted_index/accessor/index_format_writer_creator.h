#ifndef __INDEXLIB_INDEX_FORMAT_WRITER_CREATOR_H
#define __INDEXLIB_INDEX_FORMAT_WRITER_CREATOR_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_writer.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_writer.h"
#include "indexlib/index/normal/inverted_index/format/posting_format.h"

DECLARE_REFERENCE_CLASS(util, SimplePool);
DECLARE_REFERENCE_STRUCT(index, PostingWriterResource);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
IE_NAMESPACE_BEGIN(index);

class IndexFormatWriterCreator
{
public:
    IndexFormatWriterCreator(const IndexFormatOptionPtr& indexFormatOption,
                             const config::IndexConfigPtr& indexConfig);
    
    ~IndexFormatWriterCreator();
public:
    index::PostingWriter* CreatePostingWriter(
            index::PostingWriterResource* postingWriterResource);
    
    index::DictionaryWriter* CreateDictionaryWriter(autil::mem_pool::PoolBase* pool);
    index::BitmapIndexWriter* CreateBitmapIndexWriter(bool isRealTime,
            autil::mem_pool::Pool* byteSlicePool, util::SimplePool* simplePool);
    index::SectionAttributeWriterPtr CreateSectionAttributeWriter(
            util::BuildResourceMetrics* buildResourceMetrics);
private:
    config::IndexConfigPtr mIndexConfig;
    IndexFormatOptionPtr mIndexFormatOption;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexFormatWriterCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_FORMAT_WRITER_CREATOR_H
