#include "indexlib/index/common/field_format/date/test/DateTermMaker.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, DateTermMaker);

DateTermMaker::DateTermMaker() {}

DateTermMaker::~DateTermMaker() {}

void DateTermMaker::MakeDateTerms(uint64_t left, uint64_t right, std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(i, 0, 0, 0, 0, 0);
        ans.back().SetLevelType(12);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}
void DateTermMaker::MakeDateTerms(uint64_t year, uint64_t left, uint64_t right, std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, i, 0, 0, 0, 0);
        ans.back().SetLevelType(11);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}
void DateTermMaker::MakeDateTerms(uint64_t year, uint64_t month, uint64_t left, uint64_t right,
                                  std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, month, i, 0, 0, 0);
        ans.back().SetLevelType(9);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}
void DateTermMaker::MakeDateTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t left, uint64_t right,
                                  std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, month, day, i, 0, 0);
        ans.back().SetLevelType(7);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}
void DateTermMaker::MakeDateTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t left,
                                  uint64_t right, std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, month, day, hour, i, 0);
        ans.back().SetLevelType(5);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}
void DateTermMaker::MakeDateTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t minute,
                                  uint64_t left, uint64_t right, std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, month, day, hour, minute, i);
        ans.back().SetLevelType(3);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}
void DateTermMaker::MakeDateTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t minute,
                                  uint64_t second, uint64_t left, uint64_t right, std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, month, day, hour, minute, second, i);
        ans.back().SetLevelType(1);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}

void DateTermMaker::MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t left, uint64_t right,
                                    std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, month, i << 2, 0, 0, 0, 0);
        ans.back().SetLevelType(10);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}

void DateTermMaker::MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t left, uint64_t right,
                                    std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, month, day, i << 2, 0, 0, 0);
        ans.back().SetLevelType(8);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}

void DateTermMaker::MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t left,
                                    uint64_t right, std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, month, day, hour, i << 3, 0, 0);
        ans.back().SetLevelType(6);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}
void DateTermMaker::MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t minute,
                                    uint64_t left, uint64_t right, std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, month, day, hour, minute, i << 3, 0);
        ans.back().SetLevelType(4);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}

void DateTermMaker::MakeMiddleTerms(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t minute,
                                    uint64_t second, uint64_t left, uint64_t right, std::vector<uint64_t>& ret)
{
    std::vector<DateTerm> ans;
    for (uint64_t i = left; i <= right; i++) {
        ans.emplace_back(year, month, day, hour, minute, second, i << 5);
        ans.back().SetLevelType(2);
    }
    for (auto& t : ans) {
        ret.push_back(t.GetKey());
    }
}

} // namespace indexlib::index
