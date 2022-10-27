#include "build_service/util/MemUtil.h"
#include <unistd.h>

using namespace std;

namespace build_service {
namespace util {

uint64_t MemUtil::getMachineTotalMem()
{
    uint64_t pages = sysconf(_SC_PHYS_PAGES);
    uint64_t page_size = sysconf(_SC_PAGE_SIZE);
    return pages * page_size;
}

}
}
