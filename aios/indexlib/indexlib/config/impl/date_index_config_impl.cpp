#include "indexlib/config/impl/date_index_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, DateIndexConfigImpl);

DateIndexConfigImpl::DateIndexConfigImpl(const string& indexName, IndexType indexType)
    : SingleFieldIndexConfigImpl(indexName, indexType)
    , mBuildGranularity(DateLevelFormat::SECOND)
{
    mDateLevelFormat.Init(mBuildGranularity);
}

DateIndexConfigImpl::DateIndexConfigImpl(const DateIndexConfigImpl& other)
    : SingleFieldIndexConfigImpl(other)
    , mBuildGranularity(other.mBuildGranularity)
    , mDateLevelFormat(other.mDateLevelFormat)
{
}

DateIndexConfigImpl::~DateIndexConfigImpl() 
{
}

void DateIndexConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    SingleFieldIndexConfigImpl::Jsonize(json);
    if (json.GetMode() == TO_JSON)
    {
        string granularityStr = GranularityToString(mBuildGranularity);
        json.Jsonize(BUILD_GRANULARITY, granularityStr);
    }
    else
    {
        string granularityStr;
        json.Jsonize(BUILD_GRANULARITY, granularityStr, "second");
        mBuildGranularity = StringToGranularity(granularityStr);
        mDateLevelFormat.Init(mBuildGranularity);
    }
}

void DateIndexConfigImpl::AssertEqual(const IndexConfigImpl& other) const
{
    SingleFieldIndexConfigImpl::AssertEqual(other);
    const DateIndexConfigImpl& other2 = (const DateIndexConfigImpl&) other;
    IE_CONFIG_ASSERT_EQUAL(mBuildGranularity, other2.mBuildGranularity,
                           "build granularity  not equal");
}

void DateIndexConfigImpl::Check() const
{
    SingleFieldIndexConfigImpl::Check();
    if (mBuildGranularity == DateLevelFormat::GU_UNKOWN)
    {
        INDEXLIB_FATAL_ERROR(Schema, 
                             "unkown granularity");
    }
    if (mFieldConfig->IsMultiValue())
    {
        INDEXLIB_FATAL_ERROR(Schema, 
                             "date field can not be multi value field");
    }

    if (IsHashTypedDictionary())
    {
        INDEXLIB_FATAL_ERROR(Schema,
                             "date index should not set use_hash_typed_dictionary");
    }
}

IndexConfigImpl* DateIndexConfigImpl::Clone() const
{
    return new DateIndexConfigImpl(*this);
}


string DateIndexConfigImpl::GranularityToString(DateIndexConfigImpl::Granularity granularity)
{
    switch (granularity)
    {
    case DateLevelFormat::MILLISECOND:
        return "millisecond";
    case DateLevelFormat::SECOND:
        return "second";        
    case DateLevelFormat::MINUTE:
        return "minute";        
    case DateLevelFormat::HOUR:
        return "hour";        
    case DateLevelFormat::DAY:
        return "day";
    case DateLevelFormat::MONTH:
        return "month";
    case DateLevelFormat::YEAR:
        return "year";
    default:
        return "unkown";
    }
}

DateLevelFormat::Granularity DateIndexConfigImpl::StringToGranularity(const string& str)
{
    if (str == "millisecond")
    {
        return DateLevelFormat::MILLISECOND;
    }
    else if (str == "second")
    {
        return DateLevelFormat::SECOND;
    }
    else if (str == "minute")
    {
        return DateLevelFormat::MINUTE;
    }
    else if (str == "hour")
    {
        return DateLevelFormat::HOUR;
    }
    else if (str == "day")
    {
        return DateLevelFormat::DAY;
    }
    else if (str == "month")
    {
        return DateLevelFormat::MONTH;
    }
    else if (str == "year")
    {
        return DateLevelFormat::YEAR;
    }
    return DateLevelFormat::GU_UNKOWN;
}

IE_NAMESPACE_END(config);

