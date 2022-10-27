#ifndef __INDEXLIB_SORT_PATTERN_TRANSFORMER_H
#define __INDEXLIB_SORT_PATTERN_TRANSFORMER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(config);

class SortPatternTransformer
{
public:
    typedef std::vector<std::string> StringVec;
    typedef std::vector<SortPattern> SortPatternVec;

public:
    SortPatternTransformer();
    ~SortPatternTransformer();

public:
    static void FromStringVec(
            const StringVec& sortPatternStrVec, 
            SortPatternVec& sortPatterns);

    static void ToStringVec(
            const SortPatternVec& sortPatterns,
            StringVec& sortPatternStrVec);

    static SortPattern FromString(
            const std::string& sortPatternStr);

    static std::string ToString(
            const SortPattern& sortPattern); 

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortPatternTransformer);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SORT_PATTERN_TRANSFORMER_H
