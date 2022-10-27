#include "build_service/reader/FilePatternParser.h"
#include "build_service/reader/FilePattern.h"
#include "build_service/reader/EmptyFilePattern.h"
#include <autil/StringUtil.h>
#include <algorithm>
#include <set>

using namespace std;
using namespace autil;
namespace build_service {
namespace reader {

BS_LOG_SETUP(reader, FilePatternParser);

const string FilePatternParser::FILE_PATTERN_PREFIX_SEP = "<";
const string FilePatternParser::FILE_PATTERN_SURFIX_SEP = ">";
const string FilePatternParser::FILE_PATTERN_SEP = ";";
const string FilePatternParser::FILE_PATTERN_PATH_SEP = "/";

FilePatterns FilePatternParser::parse(const string &filePatternStr, const string &rootDir) {
    vector<string> patternStrVec;
    patternStrVec = StringUtil::split(filePatternStr, FILE_PATTERN_SEP);
    normalizeFilePatterns(patternStrVec);
    dedupFilePatterns(patternStrVec);

    set<string> uniqPatternSet;
    FilePatterns filePatterns;
    for (size_t i = 0; i < patternStrVec.size(); ++i) {
        FilePattern *pattern = parseFilePattern(patternStrVec[i]);
        if (!pattern) {
            filePatterns.clear();
            return filePatterns;
        }
        pattern->setRootPath(rootDir);
        filePatterns.push_back(FilePatternBasePtr(pattern));
        string patternKey = pattern->getDirPath()
                            + ":"
                            + pattern->getPrefix()
                            + ":"
                            + pattern->getSuffix();
        if (uniqPatternSet.find(patternKey) != uniqPatternSet.end()) {
            string errorMsg = "[" + filePatternStr + "] patterns with same patternKey["
                              + patternKey + "] has different file Count";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            filePatterns.clear();
            return filePatterns;
        } else {
            uniqPatternSet.insert(patternKey);
        }
    }
    if (filePatterns.empty()) {
        filePatterns.push_back(FilePatternBasePtr(new EmptyFilePattern));
    }
    return filePatterns;
}

void FilePatternParser::normalizeFilePatterns(vector<string>& filePatterns) {
    for(size_t i = 0; i < filePatterns.size(); ++i) {
        string filePatternStr = filePatterns[i];
        vector<string> splitItems;
        splitItems = StringUtil::split(filePatternStr, FILE_PATTERN_PATH_SEP);
        filePatterns[i] = StringUtil::toString(splitItems, FILE_PATTERN_PATH_SEP);
    }
}

void FilePatternParser::dedupFilePatterns(vector<string>& filePatterns) {
    if (filePatterns.size() <= 1) {
        return;
    }
    std::sort(filePatterns.begin(), filePatterns.end());
    vector<string>::iterator pos = std::unique(
            filePatterns.begin(), filePatterns.end());
    filePatterns.resize(std::distance(filePatterns.begin(), pos));
}

FilePattern* FilePatternParser::parseFilePattern(const string &patternStr) {
    size_t pos0 = patternStr.rfind(FILE_PATTERN_PATH_SEP);
    string subFilePattern;
    string patternDir;
    if (pos0 != string::npos) {
        patternDir = patternStr.substr(0, pos0 + 1);
        subFilePattern = patternStr.substr(pos0 + 1);
    } else {
        subFilePattern = patternStr;
    }
    size_t pos1 = subFilePattern.find(FILE_PATTERN_PREFIX_SEP);
    size_t pos2 = subFilePattern.find_last_of(FILE_PATTERN_SURFIX_SEP);
    if (pos1 == string::npos || pos2 == string::npos
        || pos1 >= pos2)
    {
        string errorMsg = "parse pattern " + patternStr + " fail";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    string idStr = subFilePattern.substr(pos1 + 1, pos2 - pos1 - 1);
    uint32_t fileCount;
    if (!StringUtil::strToUInt32(idStr.c_str(), fileCount)) {
        string errorMsg = "parse fileCount " + idStr + " fail";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    if (fileCount == 0) {
        string errorMsg = "fileCount can't be zero" ;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    string prefix = subFilePattern.substr(0, pos1);
    string suffix = subFilePattern.substr(pos2 + 1);
    return new FilePattern(patternDir, prefix, suffix, fileCount);
}

}
}
