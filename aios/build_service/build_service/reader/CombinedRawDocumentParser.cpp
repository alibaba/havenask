#include "build_service/reader/CombinedRawDocumentParser.h"

using namespace std;

IE_NAMESPACE_USE(document);

namespace build_service {
namespace reader {
BS_LOG_SETUP(build_service, CombinedRawDocumentParser);

bool CombinedRawDocumentParser::parse(const string &docString,
                                      RawDocument &rawDoc)
{
    size_t curParser = _lastParser;
    size_t retryCount = 0;
    do {
        if (_parsers[curParser]->parse(docString, rawDoc)) {
            _lastParser = curParser;
            return true;
        } else {
            rawDoc.clear();
            curParser = (curParser + 1) % _parserCount;
            retryCount ++;
        }
    } while (retryCount < _parserCount);
    return false;
}

}
}
