#include <autil/StringUtil.h>
#include "build_service/reader/MultiFileDocumentReader.h"
#include "build_service/reader/BinaryFileRawDocumentReader.h"
#include "build_service/reader/FileListCollector.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/reader/MultiFileDocumentReader.h"
#include "build_service/reader/DocumentSeparators.h"
#include "build_service/reader/ParserCreator.h"
#include "build_service/proto/ParserConfig.h"

using namespace std;
using namespace autil;
using namespace build_service::proto;

namespace build_service {
namespace reader {
BS_LOG_SETUP(reader, BinaryFileRawDocumentReader);

BinaryFileRawDocumentReader::BinaryFileRawDocumentReader(bool isFixLen)
    : _isFixLen(isFixLen)
{}

BinaryFileRawDocumentReader::~BinaryFileRawDocumentReader() {
}

MultiFileDocumentReader *BinaryFileRawDocumentReader::createMultiFileDocumentReader(
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
        string errorMsg = "binary file source supports only one ParserConfig,"
                          "while parserConfig size = "
                          + StringUtil::toString(parserConfigs.size());
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL; 
    }

    std::unique_ptr<MultiFileDocumentReader> reader(new MultiFileDocumentReader(collectResults));
    if (_isFixLen) {
        string lengthStr = getValueFromKeyValueMap(kvMap, RAW_DOCUMENT_BINARY_LENGTH);
        uint32_t fixLen = 0;
        if (!StringUtil::numberFromString(lengthStr, fixLen)) {
            string errorMsg = "bad parameter: " + RAW_DOCUMENT_BINARY_LENGTH + "[" + lengthStr + "]";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return NULL;
        }
        if (!reader->initForFixLenBinaryDocument(fixLen))
        {
            string errorMsg = "init multi file document reader failed";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return NULL;        
        }
    } else {
        if (!reader->initForVarLenBinaryDocument())
        {
            string errorMsg = "init multi file document reader failed";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return NULL;        
        }
    } 
    return reader.release();
}


}
}
