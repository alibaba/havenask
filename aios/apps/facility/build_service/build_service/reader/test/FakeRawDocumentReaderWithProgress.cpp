#include "build_service/reader/test/FakeRawDocumentReaderWithProgress.h"

#include "autil/StringUtil.h"
#include "build_service/reader/StandardRawDocumentParser.h"

using namespace std;
using namespace autil;
using namespace build_service::document;
namespace build_service { namespace reader {

BS_LOG_SETUP(reader, FakeRawDocumentReaderWithProgress);

// 模拟有两个swift partition的raw document reader
FakeRawDocumentReaderWithProgress::FakeRawDocumentReaderWithProgress() : _cursor(0), _resetParser(true), _hasLimit(true)
{
}

FakeRawDocumentReaderWithProgress::~FakeRawDocumentReaderWithProgress() {}

bool FakeRawDocumentReaderWithProgress::init(const ReaderInitParam& params)
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

bool FakeRawDocumentReaderWithProgress::seek(const Checkpoint& checkpoint)
{
    for (size_t i = 0; i < _documents.size(); ++i) {
        std::vector<indexlibv2::base::Progress> progress;
        progress.push_back({0, 32767, {i / 2 + 1, 0}});
        progress.push_back({32768, 65535, {i / 2 + i % 2, 0}});
        if (progress == checkpoint.progress[0]) {
            _cursor = i + 1;
            if (checkpoint.offset != (i + 1) / 2) {
                return false;
            }
            return true;
        }
    }
    return false;
}

RawDocumentReader::ErrorCode FakeRawDocumentReaderWithProgress::readDocStr(string& docStr, Checkpoint* checkpoint,
                                                                           DocInfo& docInfo)
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

    checkpoint->offset = (_documents[_cursor].offset + 1) / 2;
    docInfo.hashId = _cursor;
    checkpoint->progress.clear();
    checkpoint->progress.emplace_back();
    checkpoint->progress[0].push_back({0, 32767, {_documents[_cursor].offset / 2 + 1, 0}});
    checkpoint->progress[0].push_back(
        {32768, 65535, {_documents[_cursor].offset / 2 + _documents[_cursor].offset % 2, 0}});
    _cursor++;
    return ERROR_NONE;
}

bool FakeRawDocumentReaderWithProgress::isEof() const { return _cursor >= _documents.size(); }

void FakeRawDocumentReaderWithProgress::addRawDocument(const std::string& docStr, int64_t offset, bool readError)
{
    RawDocDesc doc;
    doc.docStr = docStr;
    doc.offset = offset;
    doc.readError = readError;
    _documents.push_back(doc);
}
}} // namespace build_service::reader
