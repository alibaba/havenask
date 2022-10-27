#ifndef ISEARCH_BS_ENCODECONVERTER_H
#define ISEARCH_BS_ENCODECONVERTER_H

#include "build_service/util/Log.h"

namespace build_service {
namespace analyzer {

class EncodeConverter
{
public:
    EncodeConverter();
    ~EncodeConverter();
public:
    static int32_t utf8ToUtf16(const char *in, int32_t length, uint16_t *out);
    static int32_t utf16ToUtf8(const uint16_t *in, int32_t length, char *out);
    static int32_t utf8ToUtf16Len(const char *in, int32_t length);
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_ENCODECONVERTER_H
