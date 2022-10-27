#include "indexlib/common/field_format/date/date_query_parser.h"
#include "indexlib/common/number_term.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, DateQueryParser);

DateQueryParser::DateQueryParser()
    : mSearchGranularityUnit(0)
{
}

DateQueryParser::~DateQueryParser() 
{
}

void DateQueryParser::Init(const DateIndexConfigPtr& dateIndexConfig)
{
    mSearchGranularityUnit =
        DateTerm::GetSearchGranularityUnit(
                dateIndexConfig->GetBuildGranularity());
    mFormat = dateIndexConfig->GetDateLevelFormat();
}


bool DateQueryParser::ParseQuery(const common::Term& term,
                                 uint64_t &leftTerm, uint64_t &rightTerm)
{
    Int64Term* rangeTerm = (Int64Term*)(&term);
    bool leftIsZero = false;
    int64_t leftTimestamp = rangeTerm->GetLeftNumber();
    int64_t rightTimestamp = rangeTerm->GetRightNumber();
    if (unlikely(leftTimestamp <= 0))
    {
        leftTimestamp = 0;
        leftIsZero = true;
    }

    if (unlikely(rightTimestamp < 0 || leftTimestamp > rightTimestamp))
    {
        return false;
    }

    if (!leftIsZero)
    {
        leftTimestamp--;
    }
    DateTerm left = DateTerm::ConvertToDateTerm(
            TimestampUtil::ConvertToDate((uint64_t)leftTimestamp), mFormat);
    DateTerm right = DateTerm::ConvertToDateTerm(
            TimestampUtil::ConvertToDate((uint64_t)rightTimestamp), mFormat);
    
    if (!leftIsZero)
    {
        left.PlusUnit(mSearchGranularityUnit);
    }
    leftTerm = left.GetKey();
    rightTerm = right.GetKey();
    return true;
}

IE_NAMESPACE_END(common);

