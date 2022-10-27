#ifndef ISEARCH_ENCODECONVERTER_H
#define ISEARCH_ENCODECONVERTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(util);

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
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(EncodeConverter);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_ENCODECONVERTER_H
