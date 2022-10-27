#ifndef ISEARCH_BS_FILEPATTERNPARSER_H
#define ISEARCH_BS_FILEPATTERNPARSER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/FilePattern.h"

namespace build_service {
namespace reader {

class FilePatternParser
{
private:
    FilePatternParser();
    ~FilePatternParser();
    FilePatternParser(const FilePatternParser &);
    FilePatternParser& operator=(const FilePatternParser &);
public:
    static FilePatterns parse(const std::string &filePatternStr, const std::string &rootDir);
private:
    static void normalizeFilePatterns(std::vector<std::string>& filePatterns);
    static void dedupFilePatterns(std::vector<std::string>& filePatterns);
    static FilePattern* parseFilePattern(const std::string &patternStr);
private:
    static const std::string FILE_PATTERN_PREFIX_SEP;
    static const std::string FILE_PATTERN_SURFIX_SEP;
    static const std::string FILE_PATTERN_SEP;
    static const std::string FILE_PATTERN_PATH_SEP;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FilePatternParser);

}
}

#endif //ISEARCH_BS_FILEPATTERNPARSER_H
