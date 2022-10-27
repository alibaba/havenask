#include "build_service/reader/Separator.h"

using namespace std;
namespace build_service {
namespace reader {

Separator::Separator(const string &sep) {
    _sep = sep;
    uint32_t sepLen = sep.size();
    for (int32_t i = 0; i < 256; i++) {
        _next[i] = 1;
    }
    for (uint32_t i = 0; i < sepLen; i++) {
        _next[(unsigned char)(sep[i])] = 0 - i;
    }
}

Separator::~Separator() {
}

}
}
