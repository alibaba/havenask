#include <ha3/qrs/PageDistinctSelector.h>
#include <autil/StringUtil.h>

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, PageDistinctSelector);

PageDistinctSelector::PageDistinctSelector(uint32_t pgHitNum, 
        const std::string &distKey, uint32_t distCount, 
        const std::vector<bool> &sortFlags,
        const std::vector<double> &gradeThresholds)
{
    _pgHitNum = pgHitNum;
    _distKey = distKey;
    _distCount = distCount;
    _grades = gradeThresholds;
    _sortFlags = sortFlags;
}

PageDistinctSelector::~PageDistinctSelector() {
}

END_HA3_NAMESPACE(qrs);

