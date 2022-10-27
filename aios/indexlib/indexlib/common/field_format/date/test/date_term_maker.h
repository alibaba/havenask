#ifndef __INDEXLIB_DATE_TERM_MAKER_H
#define __INDEXLIB_DATE_TERM_MAKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/date/date_term.h"


IE_NAMESPACE_BEGIN(common);

class DateTermMaker
{
public:
    DateTermMaker();
    ~DateTermMaker();
public:
    static void MakeDateTerms(uint64_t fromYear, uint64_t toYear, std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t left, uint64_t right,
                              std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t month,
                              uint64_t left, uint64_t right,
                              std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t month, uint64_t day,
                              uint64_t left, uint64_t right,
                              std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t month, uint64_t day,
                              uint64_t hour, uint64_t left, uint64_t right,
                              std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t month, uint64_t day,
                              uint64_t hour, uint64_t minute, uint64_t left,
                              uint64_t right, std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t month, uint64_t day,
                              uint64_t hour, uint64_t minute, uint64_t second,
                              uint64_t left, uint64_t right, std::vector<uint64_t>& ans);

    static void MakeMiddleTerms(uint64_t year, uint64_t month,
                                uint64_t left, uint64_t right,
                                std::vector<uint64_t>& ans);
    static void MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day,
                                uint64_t left, uint64_t right,
                                std::vector<uint64_t>& ans);
    static void MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day,
                                uint64_t hour, uint64_t left, uint64_t right,
                                std::vector<uint64_t>& ans);
    static void MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day,
                                uint64_t hour, uint64_t minute, uint64_t left,
                                uint64_t right, std::vector<uint64_t>& ans);    
    static void MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day,
                                uint64_t hour, uint64_t minute, uint64_t second,
                                uint64_t left, uint64_t right, std::vector<uint64_t>& ans);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateTermMaker);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DATE_TERM_MAKER_H
