#include "indexlib/common/term.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/summary/summary_reader.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, SummaryReader);

SummaryReader::SummaryReader(const SummarySchemaPtr& summarySchema) 
    : mSummarySchema(summarySchema)
{
}
    
SummaryReader::~SummaryReader() 
{
}

IE_NAMESPACE_END(index);

