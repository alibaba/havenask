#ifndef ISEARCH_BS_CODECONVERTER_H_
#define ISEARCH_BS_CODECONVERTER_H_

#include <cstdint>

namespace build_service {
namespace analyzer {

typedef enum { UTF8_T, UTF16_T, BIG5_T, GBK_T, UNDEF_T } ENCODING_TYPE;
typedef uint8_t U8CHAR;
typedef uint16_t U16CHAR;

static const U16CHAR defUniChar = 0x25a1;
static const U16CHAR defGbkChar = 0xa1f5;
static const U16CHAR defBigChar = 0xa1bc;

class CodeConverter {
 public:
    static U16CHAR convertGBKToUTF16(U16CHAR c);
    static U16CHAR convertUTF16ToGBK(U16CHAR c);
    static U16CHAR convertBig5ToUTF16(U16CHAR c);
    static U16CHAR convertUTF16ToBig5(U16CHAR c);
    static int32_t convertGBKToUTF16(U8CHAR* src, uint32_t len, U16CHAR* dst, uint32_t buf_len);
    static int32_t convertUTF16ToGBK(U16CHAR* src, uint32_t len, U8CHAR* dst, uint32_t buf_len);
    static int32_t convertBig5ToUTF16(U8CHAR* src, uint32_t len, U16CHAR* dst, uint32_t buf_len);
    static int32_t convertUTF16ToBig5(U16CHAR* src, uint32_t len, U8CHAR* dst, uint32_t buf_len);
    static int32_t convertUTF8ToUTF16(U8CHAR* src, uint32_t len, U16CHAR* dst, uint32_t buf_len);
    static int32_t convertUTF16ToUTF8(U16CHAR* src, uint32_t len, U8CHAR* dst, uint32_t buf_len);
    static int32_t convertUnicodeToUTF8(const uint16_t* src, int32_t src_len, char* dest, int32_t buf_len);
};
}
}

#endif  // ISEARCH_BS_CODECONVERTER_H_
