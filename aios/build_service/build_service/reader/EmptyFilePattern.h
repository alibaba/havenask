#ifndef ISEARCH_BS_EMPTYFILEPATTERN_H
#define ISEARCH_BS_EMPTYFILEPATTERN_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/FilePatternBase.h"

namespace build_service {
namespace reader {

class EmptyFilePattern : public FilePatternBase
{
public:
    EmptyFilePattern();
    ~EmptyFilePattern();
public:
    /* override */ CollectResults calculateHashIds(const CollectResults &results) const;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(EmptyFilePattern);

}
}

#endif //ISEARCH_BS_EMPTYFILEPATTERN_H
