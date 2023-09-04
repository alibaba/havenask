#include "ha3_sdk/testlib/index/FakeMetaMaker.h"

#include <cstdint>
#include <iostream>
#include <map>
#include <stdlib.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "ha3_sdk/testlib/index/FakeInDocSectionMeta.h"
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"
#include "indexlib/indexlib.h"

namespace indexlib {
namespace index {
AUTIL_LOG_SETUP(ha3, FakeMetaMaker);
using namespace std;

void FakeMetaMaker::makeFakeMeta(std::string str,
                                 FakeTextIndexReader::DocSectionMap &docSecMetaMap) {

    size_t startPos = 0;
    size_t strLen = str.length();
    while (true) {

        size_t endPos = str.find('\n', startPos);
        if (string::npos == endPos) {
            break;
        }

        // parse per doc meta infomation
        string line = str.substr(startPos, endPos - startPos);
        size_t docPos = line.find(':');
        if (docPos == string::npos) {
            break;
        }
        string strDoc = line.substr(0, docPos);
        uint32_t docId = atoi(strDoc.c_str());
        FakeTextIndexReader::DocSectionMap::iterator iter = docSecMetaMap.find((docid_t)docId);
        if (iter == docSecMetaMap.end()) {
            FakeInDocSectionMeta::DocSectionMeta docSectionMeta;
            parseDocMeta(line.substr(docPos + 1), docSectionMeta);
            docSecMetaMap.insert(std::make_pair(docid_t(docId), docSectionMeta));
        }
        startPos = endPos + 1;
        if (startPos >= strLen) {
            break;
        }
    }
}

void FakeMetaMaker::parseDocMeta(std::string str,
                                 FakeInDocSectionMeta::DocSectionMeta &docSecMeta) {
    size_t startPos = 0;
    uint32_t strLen = str.length();
    while (true) {
        size_t endPos = str.find(';', startPos);
        if (endPos == string::npos) {
            break;
        }

        // parse per field meta information
        string line = str.substr(startPos, endPos - startPos);
        size_t fieldPos = line.find('-');
        if (fieldPos == string::npos) {
            break;
        }
        string fieldStr = line.substr(0, fieldPos);
        size_t fieldLenPos = line.find('[');
        if (fieldLenPos == string::npos) {
            break;
        }
        string fieldLenStr = line.substr(fieldPos + 1, fieldLenPos - fieldPos - 1);
        int32_t fieldPosition = atoi(fieldStr.c_str());
        uint32_t fieldLen = atoi(fieldLenStr.c_str());
        std::map<int32_t, field_len_t>::iterator iter
            = docSecMeta.fieldLength.find(int32_t(fieldPosition));
        if (iter == docSecMeta.fieldLength.end()) {
            docSecMeta.fieldLength.insert(
                std::make_pair((int32_t)fieldPosition, (field_len_t)fieldLen));
            size_t secPos = line.find(']');
            string secLine = line.substr(fieldLenPos + 1, secPos - fieldLenPos - 1);
            parseFieldMeta(secLine, fieldPosition, docSecMeta.fieldAndSecionInfo);
        }

        startPos = endPos + 1;
        if (startPos >= strLen) {
            break;
        }
    }
}

void FakeMetaMaker::parseFieldMeta(std::string str,
                                   int32_t fieldPosition,
                                   FakeTextIndexReader::FieldAndSectionMap &fieldAndSecMap) {
    size_t startPos = 0;
    uint32_t strLen = str.length();
    while (true) {
        if (startPos >= strLen) {
            break;
        }
        size_t secIdPos = str.find('_', startPos);
        string strSecId = str.substr(startPos, secIdPos - startPos);
        uint32_t secId = atoi(strSecId.c_str());
        size_t secLenPos = str.find('_', secIdPos + 1);
        string strSecLen = str.substr(secIdPos + 1, secLenPos - secIdPos - 1);
        uint32_t secLen = atoi(strSecLen.c_str());
        size_t endPos = str.find(',', startPos);
        string strSecWeight;
        if (endPos == string::npos) {
            strSecWeight.append(str.substr(secLenPos + 1));
        } else {
            strSecWeight.append(str.substr(secLenPos + 1, endPos - secLenPos - 1));
        }
        uint32_t secWeight = atoi(strSecWeight.c_str());

        std::pair<int32_t, sectionid_t> p
            = std::make_pair((int32_t)fieldPosition, (sectionid_t)secId);
        FakeTextIndexReader::FieldAndSectionMap::iterator iter = fieldAndSecMap.find(p);
        if (iter == fieldAndSecMap.end()) {
            FakeInDocSectionMeta::SectionInfo secInfo;
            secInfo.sectionLength = (section_len_t)secLen;
            secInfo.sectionWeight = (section_weight_t)secWeight;
            fieldAndSecMap.insert(std::make_pair(p, secInfo));
        }

        if (endPos == string::npos) {
            break;
        } else {
            startPos = endPos + 1;
        }
    }
}

} // namespace index
} // namespace indexlib
