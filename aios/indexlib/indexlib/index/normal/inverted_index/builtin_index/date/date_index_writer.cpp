#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_writer.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/time_range_info.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_in_mem_segment_reader.h"
#include "indexlib/common/field_format/date/date_term.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DateIndexWriter);

DateIndexWriter::DateIndexWriter(
        size_t lastSegmentDistinctTermCount,
        const config::IndexPartitionOptions& options)
    : NormalIndexWriter(lastSegmentDistinctTermCount, options)
    , mTimeRangeInfo(new TimeRangeInfo)
{

}

DateIndexWriter::~DateIndexWriter() 
{
}

void DateIndexWriter::Init(const config::IndexConfigPtr& indexConfig,
                           BuildResourceMetrics* buildResourceMetrics,
                           const PartitionSegmentIteratorPtr& segIter)
{
    NormalIndexWriter::Init(indexConfig, buildResourceMetrics, segIter);
    DateIndexConfigPtr config = DYNAMIC_POINTER_CAST(DateIndexConfig, indexConfig);
    DateLevelFormat::Granularity granularity = config->GetBuildGranularity();
    mGranularityLevel = DateLevelFormat::GetGranularityLevel(granularity);
    DateIndexConfigPtr dateIndexConfig =
        DYNAMIC_POINTER_CAST(DateIndexConfig, mIndexConfig);
    mFormat = dateIndexConfig->GetDateLevelFormat();    
}

void DateIndexWriter::AddField(const Field *field)
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

    auto iterField = tokenizeField->Begin(); 

    const Section* section = *iterField;
    if (section == NULL || section->GetTokenCount() <= 0)
    {
        return;
    }

    const Token* token = section->GetToken(0);
    DateTerm term(token->GetHashKey());
    assert(term.GetLevel() == mGranularityLevel);
    term.SetLevelType(0);
    mTimeRangeInfo->Update(term.GetKey());
    NormalIndexWriter::AddField(field);
}

void DateIndexWriter::Dump(const file_system::DirectoryPtr& dir,
                           autil::mem_pool::PoolBase* dumpPool)
{
    NormalIndexWriter::Dump(dir, dumpPool);
    file_system::DirectoryPtr indexDirectory = dir->GetDirectory(
            mIndexConfig->GetIndexName(), true);
    mTimeRangeInfo->Store(indexDirectory);
}

IndexSegmentReaderPtr DateIndexWriter::CreateInMemReader()
{
    InMemNormalIndexSegmentReaderPtr indexSegmentReader = DYNAMIC_POINTER_CAST(
            InMemNormalIndexSegmentReader,            
            NormalIndexWriter::CreateInMemReader());
    return DateInMemSegmentReaderPtr(
            new DateInMemSegmentReader(indexSegmentReader, mFormat, mTimeRangeInfo));
}

IE_NAMESPACE_END(index);

