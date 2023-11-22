#include "build_service/reader/test/CustomRawDocumentReader.h"

#include <cstddef>
#include <memory>

#include "autil/Span.h"
#include "build_service/document/DocumentDefine.h"

using namespace std;
using namespace autil;

namespace build_service { namespace reader {

using namespace build_service::document;

BS_LOG_SETUP(reader, CustomRawDocumentReader);

CustomRawDocumentReader::CustomRawDocumentReader() { _cursor = 0; }

CustomRawDocumentReader::~CustomRawDocumentReader() {}

bool CustomRawDocumentReader::init(const ReaderInitParam& params)
{
    if (!RawDocumentReader::init(params)) {
        return false;
    }
    for (size_t i = 0; i < 5; i++) {
        RawDocDesc rawDoc;
        rawDoc.docId = i + 5;
        rawDoc.offset = i + 1;
        _documents.push_back(rawDoc);
    }
    return true;
}

bool CustomRawDocumentReader::seek(const Checkpoint& checkpoint)
{
    for (size_t i = 0; i < _documents.size(); ++i) {
        if (_documents[i].offset == checkpoint.offset) {
            _cursor = i + 1;
            return true;
        }
    }
    return false;
}

bool CustomRawDocumentReader::isEof() const { return _cursor >= _documents.size(); }

RawDocumentReader::ErrorCode CustomRawDocumentReader::readDocStr(string& docStr, Checkpoint* checkpoint,
                                                                 DocInfo& docInfo)
{
    if (_cursor >= _documents.size()) {
        return ERROR_EOF;
    }
    docStr = "CMD=add\x1F\n"
             "id=" +
             _documents[_cursor].docId +
             "\x1F\n"
             "\x1E\n";
    _cursor++;
    return ERROR_NONE;
}

}} // namespace build_service::reader
