#ifndef ISEARCH_BS_NORMALIZER_H
#define ISEARCH_BS_NORMALIZER_H

#include "build_service/util/Log.h"
#include "build_service/analyzer/CodeConverter.h"
#include "build_service/analyzer/AnalyzerInfo.h"
#include "build_service/analyzer/NormalizeTable.h"

namespace build_service {
namespace analyzer {

class Normalizer
{
public:
    enum {
        K_OP_NORMALIZE = 0x01,  // 需要执行normalize操作，主要用于编码转换时            
    };
public:
    Normalizer(const NormalizeOptions &options = NormalizeOptions(), const std::map<uint16_t, uint16_t> *traditionalTablePatch = NULL);
    ~Normalizer();
public:
    void normalize(const std::string &word, std::string &normalizeWord);
    void normalizeUTF16(const uint16_t *in, size_t len,
                        uint16_t *out);
    bool needNormalize() {
        return !(_options.traditionalSensitive &&
                 _options.widthSensitive &&
                 _options.caseSensitive);
    }

public:
   bool gbkToUtf8(const std::string &input, std::string &output, unsigned op = 0) const;

   bool utf8ToGbk(const std::string &input, std::string &output, unsigned op = 0) const;

   bool strNormalizeUtf8(const std::string &input, std::string &output) const;

   bool strNormalizeGbk(const std::string &input, std::string &output) const;

   bool unicodeToUtf8(const std::string &input, std::string &output) const;

private:
   typedef int (*TO_UNICODE_FUN)(U8CHAR *, unsigned int, U16CHAR *, unsigned int);
   typedef int (*FROM_UNICODE_FUN)(U16CHAR *, unsigned int, U8CHAR *, unsigned int);
   bool stringConverter(const std::string &input, std::string &output, TO_UNICODE_FUN to_unicode,
                        FROM_UNICODE_FUN from_unicode, unsigned op = 0) const;

private:
    NormalizeOptions _options;
    NormalizeTable _table;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Normalizer);

}
}

#endif //ISEARCH_BS_NORMALIZER_H
