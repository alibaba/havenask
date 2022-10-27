#include "build_service/util/UTF8Util.h"

using namespace std;

namespace build_service {
namespace util {
BS_LOG_SETUP(util, UTF8Util);

static const unsigned char gU8Mask[6] = {0x80, 0xE0, 0xF0, 0xF8, 0xFC, 0xFE};
static const unsigned char gU8Mark[6] = {0x00, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC};


std::string UTF8Util::getNextCharUTF8(const std::string& str, size_t start) {
    if (start < str.length()) {
        unsigned char value = str[start];
        unsigned int i;
        for (i = 0U; i < 6U; ++i) {
            if ((value & gU8Mask[i]) == gU8Mark[i]) {
                if (start + i < str.length()) {
                    return str.substr(start, i + 1);
                } else {
                    break;
                }
            }
        }
    }

    return "";
}

/* valid utf-8 format
 * 0xxxxxxx                                        (00-7f）
 * 110xxxxx 10xxxxxx                               (c0-df)(80-bf)
 * 1110xxxx 10xxxxxx 10xxxxxx                      (e0-ef)(80-bf)(80-bf)
 * 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx              (f0-f7)(80-bf)(80-bf)(80-bf)
 * 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx     (f8-fb)(80-bf)(80-bf)(80-bf)(80-bf)
 * 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx (fc-fd)(80-bf)(80-bf)(80-bf)(80-bf)(80-bf)
 * Checking the string from 3 points.
 * 1. A token has 1-6 bytes.
 * 2. In a token, the first 2 bits of the bytes except the first byte should be 10xxxxx.
 * 3. The count of the bytes should follow the first byte.
 */
bool UTF8Util::getNextCharUTF8(const char *str, size_t start, size_t end, size_t &len) {
    if (start < end) {
        unsigned char value = str[start];
        for (unsigned int i = 0U; i != 6U; ++i) {
            if ((value & gU8Mask[i]) == gU8Mark[i]) {
                if (start + i < end) {
                    len = i + 1;
                    return true;
                }
                return false;
            }
        }
    }
    return false;
}

}
}
