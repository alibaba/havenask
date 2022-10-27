#include <string>
#include "build_service/analyzer/Normalizer.h"
#include "build_service/analyzer/EncodeConverter.h"
#include "build_service/analyzer/CodeConverter.h"

using namespace std;
using namespace autil;
namespace build_service {
namespace analyzer {
BS_LOG_SETUP(analyzer, Normalizer);

Normalizer::Normalizer(const NormalizeOptions &options, const map<uint16_t, uint16_t> *traditionalTablePatch)
    : _options(options)
    , _table(options.caseSensitive,
             options.traditionalSensitive,
             options.widthSensitive,
             traditionalTablePatch)
{
}

Normalizer::~Normalizer() {
}

template <class T>
class StackBuf {
public:
    StackBuf(size_t size) {
        _size = size;
        if (size > STACK_BUF_LEN) {
            _buf = new T[size];
        } else {
            _buf = _stackBuf;
        }
    }
    ~StackBuf() {
        if (_size > STACK_BUF_LEN) {
            delete[] _buf;
        }
    }

    T* getBuf() const {
        return _buf;
    }
private:
    StackBuf(const StackBuf &);
    StackBuf& operator=(const StackBuf &);
private:
    static const size_t STACK_BUF_LEN = 4 * 1024 / sizeof(T);
    size_t _size;
    T _stackBuf[STACK_BUF_LEN];
    T *_buf;
};

void Normalizer::normalize(const string &word, string &normalizeWord) {
    StackBuf<uint16_t> stackBuf16(word.size());
    uint16_t *buf16 = stackBuf16.getBuf();
    int32_t u16Len = EncodeConverter::utf8ToUtf16(word.data(), word.size(), buf16);
    normalizeUTF16(buf16, u16Len, buf16);

    StackBuf<char> stackBuf8(word.size() + 1);
    char *buf8 = stackBuf8.getBuf();

    int32_t u8Len = EncodeConverter::utf16ToUtf8(buf16, u16Len, buf8);
    buf8[u8Len] = '\0';
    normalizeWord.assign(buf8);
}

void Normalizer::normalizeUTF16(const uint16_t *in, size_t len,
                                uint16_t *out)
{
    for (size_t i = 0; i < len; ++i) {
        out[i] = _table[in[i]];
    }
}

bool Normalizer::gbkToUtf8(const string &input, string &output, unsigned op) const {
    TO_UNICODE_FUN to_unicode = CodeConverter::convertGBKToUTF16;
    FROM_UNICODE_FUN from_unicode = CodeConverter::convertUTF16ToUTF8;
    return stringConverter(input, output, to_unicode, from_unicode, op);
}

bool Normalizer::utf8ToGbk(const string &input, string &output, unsigned op) const {
    TO_UNICODE_FUN to_unicode = CodeConverter::convertUTF8ToUTF16;
    FROM_UNICODE_FUN from_unicode = CodeConverter::convertUTF16ToGBK;
    return stringConverter(input, output, to_unicode, from_unicode, op);
}

bool Normalizer::strNormalizeUtf8(const string &input, string &output) const {
    TO_UNICODE_FUN to_unicode = CodeConverter::convertUTF8ToUTF16;
    FROM_UNICODE_FUN from_unicode = CodeConverter::convertUTF16ToUTF8;
    return stringConverter(input, output, to_unicode, from_unicode, K_OP_NORMALIZE);
}

bool Normalizer::strNormalizeGbk(const string &input, string &output) const {
    TO_UNICODE_FUN to_unicode = CodeConverter::convertGBKToUTF16;
    FROM_UNICODE_FUN from_unicode = CodeConverter::convertUTF16ToGBK;
    return stringConverter(input, output, to_unicode, from_unicode, K_OP_NORMALIZE);
}

bool Normalizer::unicodeToUtf8(const string &input, string &output) const {
    if (input.empty()) {
        output.clear();
        return true;
    }
    size_t input_len = input.size();
    uint16_t *src = new uint16_t[input_len + 1];
    uint32_t index = 0;
    for (size_t i = 0; i < input_len;) {
        if (input[i] == '\\' && input[i + 1] == 'u') {
            char temp[5];
            memcpy(temp, &(input[i + 2]), 4);
            temp[4] = 0;
            src[index] = strtoul(temp, NULL, 16);
            i += 6;
            ++index;
        } else {
            src[index] = input[i];
            i += 1;
            ++index;
        }
    }
    int buffer_len = input_len + 1;
    char *buffer = new char[buffer_len];
    int len = CodeConverter::convertUnicodeToUTF8((const uint16_t *)src, index, buffer, buffer_len);
    bool ret = false;
    if (len > 0) {
        output.assign(buffer, len);
        ret = true;
    } else {
        BS_LOG(WARN, "convert unicode string [%s] to utf-8 failed.", input.c_str());
    }
    delete[] src;
    delete[] buffer;
    return ret;
}

bool Normalizer::stringConverter(const string &input, string &output, TO_UNICODE_FUN to_unicode,
                                 FROM_UNICODE_FUN from_unicode, unsigned op) const {
    if (input.empty()) {
        return true;
    }
    size_t buf_len = input.length();
    vector<uint16_t> tmp_buf(buf_len);
    vector<uint8_t> out_buf(buf_len * 2);
    uint16_t *p_tmp = &tmp_buf[0];
    uint8_t *p_out_buf = &out_buf[0];
    strncpy((char *)p_out_buf, input.c_str(), buf_len);

    int len = to_unicode(p_out_buf, buf_len, p_tmp, buf_len);
    if (0 > len) {
        return false;
    }
    // normalize
    if (op & K_OP_NORMALIZE) {
        for (int i = 0; i < len; ++i) {
            p_tmp[i] = _table[p_tmp[i]];
        }
    }
    len = from_unicode(p_tmp, len, p_out_buf, buf_len * 2);
    if (0 > len) {
        return false;
    }

    //    p_out_buf[len] = 0;
    output.assign((const char *)p_out_buf, len);

    return true;
}

}
}
