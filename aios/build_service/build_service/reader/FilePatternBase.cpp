#include "build_service/reader/FilePatternBase.h"
#include "build_service/reader/FilePattern.h"

using namespace std;

namespace build_service {
namespace reader {

bool FilePatternBase::isPatternConsistent(
        const FilePatternBasePtr &lft,
        const FilePatternBasePtr &rhs)
{
    FilePatternPtr leftPattern = std::tr1::dynamic_pointer_cast<FilePattern>(lft);
    FilePatternPtr rightPattern = std::tr1::dynamic_pointer_cast<FilePattern>(rhs);
    if (!leftPattern || !rightPattern) {
        return false;
    }
    return leftPattern->getFileCount() == rightPattern->getFileCount();
}

}
}
