#ifndef __INDEXLIB_DATE_FIELD_ENCODER_H
#define __INDEXLIB_DATE_FIELD_ENCODER_H

#include <tr1/memory>
#include <ctime>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "autil/StringUtil.h"
#include "indexlib/util/timestamp_util.h"
#include "indexlib/common/field_format/date/date_query_parser.h"

IE_NAMESPACE_BEGIN(common);

class DateFieldEncoder
{
public:
    DateFieldEncoder(const config::IndexPartitionSchemaPtr& schema);
    ~DateFieldEncoder();
public:
    void Init(const config::IndexPartitionSchemaPtr& schema);
    bool IsDateIndexField(fieldid_t fieldId);
    void Encode(fieldid_t fieldId, const std::string& fieldValue,
                std::vector<dictkey_t>& dictKeys);

private:

    typedef std::pair<config::DateIndexConfig::Granularity, config::DateLevelFormat> ConfigPair;
    std::vector<ConfigPair> mDateDescriptions;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateFieldEncoder);

inline void DateFieldEncoder::Encode(fieldid_t fieldId, const std::string& fieldValue,
                              std::vector<dictkey_t>& dictKeys)
{
    dictKeys.clear();
    if (!IsDateIndexField(fieldId))
    {
        return;
    }
    if (fieldValue.empty())
    {
        IE_LOG(DEBUG, "field [%d] value is empty.", fieldId);
        return;
    }

    uint64_t time;
    if (!autil::StringUtil::numberFromString(fieldValue, time))
    {
        IE_LOG(DEBUG, "field value [%s] is not date number.", fieldValue.c_str());
        return;
    }

    util::TimestampUtil::Date date = util::TimestampUtil::ConvertToDate(time);
    config::DateLevelFormat& format = mDateDescriptions[fieldId].second;
    DateTerm tmp = DateTerm::ConvertToDateTerm(date, format);
    DateTerm::EncodeDateTermToTerms(tmp, format, dictKeys);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DATE_FIELD_ENCODER_H

