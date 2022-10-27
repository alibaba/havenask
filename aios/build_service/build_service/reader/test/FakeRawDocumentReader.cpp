#include "build_service/reader/test/FakeRawDocumentReader.h"
#include "build_service/reader/StandardRawDocumentParser.h"

using namespace std;
using namespace build_service::document;
namespace build_service {
namespace reader {

BS_LOG_SETUP(reader, FakeRawDocumentReader);

FakeRawDocumentReader::FakeRawDocumentReader()
    : _cursor(0)
    , _resetParser(true)
{
}

FakeRawDocumentReader::~FakeRawDocumentReader() {
}

bool FakeRawDocumentReader::init(const ReaderInitParam &params) {
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

bool FakeRawDocumentReader::seek(int64_t offset) {
    for (size_t i = 0; i < _documents.size(); ++i) {
        if (_documents[i].offset == offset) {
            _cursor = i + 1;
            return true;
        }
    }
    return false;
}

RawDocumentReader::ErrorCode FakeRawDocumentReader::readDocStr(
        string &docStr, int64_t &offset, int64_t &timestamp)
{
    if (_cursor >= _documents.size()) {
        return ERROR_EOF;
    }
    if (_documents[_cursor].readError) {
        return ERROR_EXCEPTION;
    }
    docStr = _documents[_cursor].docStr;
    offset = _documents[_cursor].offset;
    _cursor++;
    return ERROR_NONE;
}

bool FakeRawDocumentReader::isEof() const {
    return _cursor >= _documents.size();
}

void FakeRawDocumentReader::addRawDocument(
        const std::string &docStr, int64_t offset, bool readError)
{
    RawDocDesc doc;
    doc.docStr = docStr;
    doc.offset = offset;
    doc.readError = readError;
    _documents.push_back(doc);
}

int64_t FakeRawDocumentReader::getFreshness() {
    return (int64_t)(_documents.size() - _cursor);
}

}
}
