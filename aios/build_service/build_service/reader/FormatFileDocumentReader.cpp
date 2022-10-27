#include "build_service/reader/FormatFileDocumentReader.h"
#include "build_service/reader/GZipFileReader.h"
#include "build_service/reader/FileReader.h"

using namespace std;
namespace build_service {
namespace reader {
BS_LOG_SETUP(reader, FormatFileDocumentReader);

FormatFileDocumentReader::FormatFileDocumentReader(
        const string &docPrefix, const string &docSuffix, uint32_t bufferSize)
    : FileDocumentReader(bufferSize)
    , _docPrefix(docPrefix)
    , _docSuffix(docSuffix)
{
    uint32_t maxPrefixLen = std::max(_docPrefix.size(), _docSuffix.size());
    _bufferSize = _bufferSize < maxPrefixLen ? maxPrefixLen : _bufferSize;
}

FormatFileDocumentReader::~FormatFileDocumentReader() {}

bool FormatFileDocumentReader::doRead(std::string& docStr) {
    if (!findSeparator(_docPrefix, docStr)) {
        return false;
    }
    docStr.clear();
    docStr.reserve(32 * 1024);
    return findSeparator(_docSuffix, docStr);
}

bool FormatFileDocumentReader::findSeparator(
        const Separator &separator, string &docStr)
{
    uint32_t sepLen = separator.size();
    if (sepLen == 0) {
        return true;
    }

    while (true) {
        char *pos = (char*)separator.findInBuffer(_bufferCursor, _bufferEnd);
        if (pos != NULL) {
            readUntilPos(pos, sepLen, docStr);
            return true;
        }
        char *readEnd = _bufferEnd - sepLen;
        if (readEnd > _bufferCursor) {
            readUntilPos(readEnd, 0, docStr);
        }
        if (!fillBuffer()) {
            return false;
        }
    }
}

}
}
