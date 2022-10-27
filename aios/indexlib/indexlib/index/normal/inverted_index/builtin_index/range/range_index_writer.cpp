#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_info.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_in_mem_segment_reader.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/config/range_index_config.h"

using namespace std;
using namespace autil;


IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, RangeIndexWriter);

RangeIndexWriter::RangeIndexWriter(
        const std::string& indexName,
        index_base::SegmentMetricsPtr segmentMetrics,
        const config::IndexPartitionOptions& options)
    : mBasePos(0)
    , mRangeInfo(new RangeInfo)
{
    size_t lastSegmentDistinctTermCount =
        segmentMetrics->GetDistinctTermCount(
                RangeIndexConfig::GetBottomLevelIndexName(indexName));
    mBottomLevelWriter.reset(new NormalIndexWriter(lastSegmentDistinctTermCount, options));
    lastSegmentDistinctTermCount =
        segmentMetrics->GetDistinctTermCount(
                RangeIndexConfig::GetHighLevelIndexName(indexName));
    mHighLevelWriter.reset(new NormalIndexWriter(lastSegmentDistinctTermCount, options));
}

RangeIndexWriter::~RangeIndexWriter() 
{
}

void RangeIndexWriter::Init(const IndexConfigPtr& indexConfig,
                            BuildResourceMetrics* buildResourceMetrics,
                            const PartitionSegmentIteratorPtr& segIter)
{
    mIndexConfig = indexConfig;
    string indexName = indexConfig->GetIndexName();
    RangeIndexConfigPtr rangeIndexConfig =
        DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
    IndexConfigPtr bottomLevelConfig =
        rangeIndexConfig->GetBottomLevelIndexConfig();
    mBottomLevelWriter->Init(bottomLevelConfig, buildResourceMetrics, segIter);
    
    IndexConfigPtr highLevelConfig = rangeIndexConfig->GetHighLevelIndexConfig();
    mHighLevelWriter->Init(highLevelConfig, buildResourceMetrics, segIter);
}

size_t RangeIndexWriter::EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter)
{
    return mBottomLevelWriter->EstimateInitMemUse(indexConfig, segIter)
        + mHighLevelWriter->EstimateInitMemUse(indexConfig, segIter);
}

void RangeIndexWriter::FillDistinctTermCount(SegmentMetricsPtr mSegmentMetrics)
{
    mBottomLevelWriter->FillDistinctTermCount(mSegmentMetrics);
    mHighLevelWriter->FillDistinctTermCount(mSegmentMetrics);
}

IndexSegmentReaderPtr RangeIndexWriter::CreateInMemReader()
{
    InMemNormalIndexSegmentReaderPtr bottomReader = DYNAMIC_POINTER_CAST(
            InMemNormalIndexSegmentReader,
            mBottomLevelWriter->CreateInMemReader());
    InMemNormalIndexSegmentReaderPtr highReader = DYNAMIC_POINTER_CAST(
            InMemNormalIndexSegmentReader,
            mHighLevelWriter->CreateInMemReader());
    return IndexSegmentReaderPtr(
            new RangeInMemSegmentReader(bottomReader, highReader, mRangeInfo));
}

void RangeIndexWriter::AddField(const document::Field* field)
{
    auto tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
    if (!tokenizeField)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "fieldTag [%d] is not IndexTokenizeField",
                             static_cast<int8_t>(field->GetFieldTag()));
    }
    if (tokenizeField->GetSectionCount() <= 0)
    {
        return;
    }
    assert(tokenizeField->GetSectionCount() == 1);
    auto iterField = tokenizeField->Begin(); 

    const Section* section = *iterField;
    if (section == NULL || section->GetTokenCount() <= 0)
    {
        return;
    }
    const Token* token = section->GetToken(0);
    mRangeInfo->Update(token->GetHashKey());

    pos_t tokenBasePos = mBasePos + token->GetPosIncrement();
    fieldid_t fieldId = tokenizeField->GetFieldId();    
    mBottomLevelWriter->AddToken(token, fieldId, tokenBasePos);
    for (size_t i = 1; i < section->GetTokenCount(); i++)
    {
        const Token* token = section->GetToken(i);
        tokenBasePos += token->GetPosIncrement();
        mHighLevelWriter->AddToken(token, fieldId, tokenBasePos);
    }
    mBasePos += section->GetLength();
}

void RangeIndexWriter::EndDocument(const IndexDocument& indexDocument)
{
    mBottomLevelWriter->EndDocument(indexDocument);
    mHighLevelWriter->EndDocument(indexDocument);
    mBasePos = 0;
}

void RangeIndexWriter::EndSegment()
{
    mBottomLevelWriter->EndSegment();
    mHighLevelWriter->EndSegment();
}

void RangeIndexWriter::Dump(const file_system::DirectoryPtr& dir,
                            mem_pool::PoolBase* dumpPool)
{
    file_system::DirectoryPtr indexDirectory = dir->MakeDirectory(
            mIndexConfig->GetIndexName());
    mBottomLevelWriter->Dump(indexDirectory, dumpPool);
    mHighLevelWriter->Dump(indexDirectory, dumpPool);
    mRangeInfo->Store(indexDirectory);    
}

IE_NAMESPACE_END(index);

