#include "build_service/reader/test/FakeRawDocumentReader.h"

#include "autil/StringUtil.h"
#include "build_service/reader/StandardRawDocumentParser.h"

using namespace std;
using namespace autil;
using namespace build_service::document;
namespace build_service { namespace reader {

BS_LOG_SETUP(reader, FakeRawDocumentReader);

FakeRawDocumentReader::FakeRawDocumentReader() : _cursor(0), _resetParser(true), _hasLimit(true) {}

FakeRawDocumentReader::~FakeRawDocumentReader() {}

bool FakeRawDocumentReader::init(const ReaderInitParam& params)
{
    if (!RawDocumentReader::init(params)) {
        return false;
    }
    if (!_resetParser) {
        return true;
    }
    delete _parser;
    _parser = new StandardRawDocumentParser("\n", "=");
    return true;
}

bool FakeRawDocumentReader::seek(const Checkpoint& checkpoint)
{
    for (size_t i = 0; i < _documents.size(); ++i) {
        if (_documents[i].offset == checkpoint.offset) {
            _cursor = i + 1;
            return true;
        }
    }
    return false;
}

RawDocumentReader::ErrorCode FakeRawDocumentReader::readDocStr(string& docStr, Checkpoint* checkpoint, DocInfo& docInfo)
{
    if (_cursor >= _documents.size()) {
        if (_hasLimit) {
            return ERROR_EOF;
        } else {
            checkpoint->offset = TimeUtility::currentTime();
            return ERROR_WAIT;
        }
    }
    if (_documents[_cursor].readError) {
        return ERROR_EXCEPTION;
    }
    docStr = _documents[_cursor].docStr;
    checkpoint->offset = _documents[_cursor].offset;
    docInfo.hashId = _cursor;
    _cursor++;
    return ERROR_NONE;
}

bool FakeRawDocumentReader::isEof() const { return _cursor >= _documents.size(); }

void FakeRawDocumentReader::addRawDocument(const std::string& docStr, int64_t offset, bool readError)
{
    RawDocDesc doc;
    doc.docStr = docStr;
    doc.offset = offset;
    doc.readError = readError;
    _documents.push_back(doc);
}

}} // namespace build_service::reader
