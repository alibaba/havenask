#ifndef __INDEXLIB_DATE_QUERY_PARSER_H
#define __INDEXLIB_DATE_QUERY_PARSER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/term.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/common/field_format/date/date_term.h"

IE_NAMESPACE_BEGIN(common);

class DateQueryParser
{
public:
    DateQueryParser();
    ~DateQueryParser();
public:
    void Init(const config::DateIndexConfigPtr& dateIndexConfig);
    bool ParseQuery(const common::Term& term,
                    uint64_t &leftTerm, uint64_t &rightTerm);
private:
    uint64_t mSearchGranularityUnit;
    config::DateLevelFormat mFormat;

private:
    friend class DateQueryParserTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateQueryParser);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DATE_QUERY_PARSER_H
