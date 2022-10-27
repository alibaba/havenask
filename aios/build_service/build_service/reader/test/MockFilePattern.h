#ifndef ISEARCH_BS_MOCKFILEPATTERN_H
#define ISEARCH_BS_MOCKFILEPATTERN_H

#include "build_service/reader/FilePatternBase.h"

namespace build_service {
namespace reader {

class MockFilePattern : public FilePatternBase
{
public:
    MockFilePattern()
        : FilePatternBase("")
    {}
    ~MockFilePattern() {}
public:
    MOCK_CONST_METHOD1(calculateHashIds, CollectResults(const CollectResults &));
};

}
}

#endif // ISEARCH_BS_MOCKFILEPATTERN_H
