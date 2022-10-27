#include "indexlib/index/normal/inverted_index/accessor/index_format_writer_creator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexFormatWriterCreator);

IndexFormatWriterCreator::IndexFormatWriterCreator(
        const IndexFormatOptionPtr& indexFormatOption,
        const IndexConfigPtr& indexConfig)
{
    mIndexConfig = indexConfig;
    mIndexFormatOption = indexFormatOption;
}

IndexFormatWriterCreator::~IndexFormatWriterCreator() 
{
}

PostingWriter* IndexFormatWriterCreator::CreatePostingWriter(
        PostingWriterResource* postingWriterResource)
{
    assert(postingWriterResource);
    IndexType indexType = mIndexConfig->GetIndexType();
    PostingWriter *postingWriter = NULL;

    switch (indexType)
    {
    case it_string:
    case it_spatial:
    case it_date:
    case it_range:
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
        postingWriter = POOL_NEW_CLASS(postingWriterResource->byteSlicePool,
                PostingWriterImpl, postingWriterResource);
        break;
    default:
        assert(false);
    }
    return postingWriter;
}

DictionaryWriter* IndexFormatWriterCreator::CreateDictionaryWriter(PoolBase* pool)
{
    return DictionaryCreator::CreateWriter(mIndexConfig, pool);
}

BitmapIndexWriter* IndexFormatWriterCreator::CreateBitmapIndexWriter(
        bool isRealTime, Pool* byteSlicePool, SimplePool* simplePool)
{
    BitmapIndexWriter *bitmapIndexWriter = NULL;
    if (mIndexFormatOption->HasBitmapIndex())
    {
        bitmapIndexWriter = POOL_NEW_CLASS(byteSlicePool, 
                BitmapIndexWriter, byteSlicePool,
                simplePool, mIndexFormatOption->IsNumberIndex(),isRealTime);
    }
    return bitmapIndexWriter;
}

SectionAttributeWriterPtr IndexFormatWriterCreator::CreateSectionAttributeWriter(
        BuildResourceMetrics* buildResourceMetrics)
{
    if (mIndexFormatOption->HasSectionAttribute())
    {
        PackageIndexConfigPtr packageIndexConfig = std::tr1::dynamic_pointer_cast<
            config::PackageIndexConfig>(mIndexConfig);
        SectionAttributeWriterPtr sectionWriter(new SectionAttributeWriter(
                        packageIndexConfig));
        sectionWriter->Init(buildResourceMetrics);
        return sectionWriter;
    }
    return SectionAttributeWriterPtr();
}

IE_NAMESPACE_END(index);

