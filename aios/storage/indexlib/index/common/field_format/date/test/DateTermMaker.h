#pragma once
#include <memory>

#include "indexlib/index/common/field_format/date/DateTerm.h"

namespace indexlib::index {

class DateTermMaker
{
public:
    DateTermMaker();
    ~DateTermMaker();

public:
    static void MakeDateTerms(uint64_t fromYear, uint64_t toYear, std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t left, uint64_t right, std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t month, uint64_t left, uint64_t right, std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t left, uint64_t right,
                              std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t left, uint64_t right,
                              std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t minute,
                              uint64_t left, uint64_t right, std::vector<uint64_t>& ans);
    static void MakeDateTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t minute,
                              uint64_t second, uint64_t left, uint64_t right, std::vector<uint64_t>& ans);

    static void MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t left, uint64_t right,
                                std::vector<uint64_t>& ans);
    static void MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t left, uint64_t right,
                                std::vector<uint64_t>& ans);
    static void MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t left,
                                uint64_t right, std::vector<uint64_t>& ans);
    static void MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t minute,
                                uint64_t left, uint64_t right, std::vector<uint64_t>& ans);
    static void MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t minute,
                                uint64_t second, uint64_t left, uint64_t right, std::vector<uint64_t>& ans);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
