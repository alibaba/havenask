#ifndef ISEARCH_BS_TEXTBUFFER_H
#define ISEARCH_BS_TEXTBUFFER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/analyzer/Normalizer.h"

namespace build_service {
namespace analyzer {

/*
 * TextBuffer is used to optimize the memory new/delete when the
 * filed text is tokenized. And this buffer is reused.
 */
class TextBuffer {
public:
    TextBuffer();
    ~TextBuffer();
public:
    /*
     * normalize the str(utf8) to _normalizedUTF8Text, and trans the str
     * to utf16 coped into _orignalUTF16Text. the _normalizedUTF16Text is
     * temp buf when encode converting, and it can be reuse in future.
     */
    void normalize(const NormalizerPtr &normalizerPtr, const char *str, size_t &len);
    bool nextTokenOrignalText(const std::string &tokenNormalizedUTF8Text,
                              std::string &tokenOrignalText);
    char* getNormalizedUTF8Text() {return _normalizedUTF8Text;}
private:
    void resize(int32_t strLen);
    void reset(int32_t strLen);
private:
    char *_buf;
    uint16_t * _orignalUTF16Text;
    uint16_t *_normalizedUTF16Text;
    char *_normalizedUTF8Text;
    int32_t _size;
    int32_t _orignalUTF16TextLen;
    int32_t _curTokenPos;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_TEXTBUFFER_H
