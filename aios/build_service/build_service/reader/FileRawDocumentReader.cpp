#include "build_service/reader/FileRawDocumentReader.h"
#include "build_service/reader/FileListCollector.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/reader/MultiFileDocumentReader.h"
#include "build_service/reader/DocumentSeparators.h"
#include "build_service/reader/ParserCreator.h"
#include "build_service/proto/ParserConfig.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Monitor.h"
#include <fslib/fslib.h>
#include <autil/HashAlgorithm.h>
#include <autil/StringUtil.h>
#include <indexlib/util/counter/state_counter.h>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace build_service::document;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::common;

namespace build_service {
namespace reader {

BS_LOG_SETUP(reader, FileRawDocumentReader);

FileRawDocumentReader::FileRawDocumentReader()
    : _documentReader(NULL)
{
}

FileRawDocumentReader::~FileRawDocumentReader() {
    DELETE_AND_SET_NULL(_documentReader);
}

bool FileRawDocumentReader::init(const ReaderInitParam &params) {
    if (!RawDocumentReader::init(params)) {
        return false;
    }

    CollectResults fileList;
    if (!FileListCollector::collect(params.kvMap, params.range, fileList)) {
        string errorMsg = "Init file reader failed : collect fileList failed, kvMap is: "
                          + keyValueMapToString(params.kvMap);
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }

    _documentReader = createMultiFileDocumentReader(fileList, params.kvMap);
    if (!_documentReader) {
        return false;
    }
    _leftTimeCounter = GET_STATE_COUNTER(params.counterMap, processor.estimateLeftTime);
    if (!_leftTimeCounter) {
        BS_LOG(ERROR, "can not get estimateLeftTime from counterMap");
    }
    return true;
}

MultiFileDocumentReader *FileRawDocumentReader::createMultiFileDocumentReader(
        const CollectResults &collectResults, const KeyValueMap &kvMap)
{
    assert(_documentFactoryWrapper);
    ParserCreator parserCreator(_documentFactoryWrapper);
    vector<ParserConfig> parserConfigs;
    if (!parserCreator.createParserConfigs(kvMap, parserConfigs)) {
        string errorMsg = "create parserConfigs failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;        
    }

    if (parserConfigs.size() != 1) {
        string errorMsg = "file source supports only one ParserConfig,"
                          "while parserConfig size = "
                          + StringUtil::toString(parserConfigs.size());
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL; 
    }

    
    string rawDocPrefix;
    string rawDocSuffix;
    string format = parserConfigs[0].type;
    if (format == RAW_DOCUMENT_HA3_DOCUMENT_FORMAT) {
        rawDocPrefix = RAW_DOCUMENT_HA3_SEP_PREFIX;
        rawDocSuffix = RAW_DOCUMENT_HA3_SEP_SUFFIX;
    } else if (format == RAW_DOCUMENT_ISEARCH_DOCUMENT_FORMAT) {
        rawDocPrefix = RAW_DOCUMENT_ISEARCH_SEP_PREFIX;
        rawDocSuffix = RAW_DOCUMENT_ISEARCH_SEP_SUFFIX;
    } else {
        rawDocPrefix = getValueFromKeyValueMap(kvMap,
                RAW_DOCUMENT_SEP_PREFIX, RAW_DOCUMENT_HA3_SEP_PREFIX);
        rawDocSuffix = getValueFromKeyValueMap(kvMap,
                RAW_DOCUMENT_SEP_SUFFIX, RAW_DOCUMENT_HA3_SEP_SUFFIX);
    }

    if (rawDocSuffix.empty()) {
        string errorMsg = "separator_suffix can not be empty!";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    std::unique_ptr<MultiFileDocumentReader> reader(new MultiFileDocumentReader(collectResults));
    if (!reader->initForFormatDocument(rawDocPrefix, rawDocSuffix))
    {
        string errorMsg = "init multi file document reader failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;        
    }
    return reader.release();
}

bool FileRawDocumentReader::seek(int64_t offset) {
    return _documentReader->seek(offset);
}

RawDocumentReader::ErrorCode FileRawDocumentReader::readDocStr(
        string &docStr, int64_t &offset, int64_t &timestamp)
{
    bool ret = _documentReader->read(docStr, offset);
    if (!ret) {
        return _documentReader->isEof() ? ERROR_EOF : ERROR_EXCEPTION;
    }
    int64_t estimateLeftTime = _documentReader->estimateLeftTime() / (1000 * 1000);
    if (_leftTimeCounter) {
        _leftTimeCounter->Set(estimateLeftTime);
    }
    return ERROR_NONE;
}


bool FileRawDocumentReader::isEof() const {
    return _documentReader->isEof();
}

int64_t FileRawDocumentReader::getFreshness() {
    return _documentReader->estimateLeftTime();
}

}
}
