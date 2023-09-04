#include "ha3_sdk/testlib/index/FakePostingMaker.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <stdlib.h>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"
#include "indexlib/indexlib.h"

namespace indexlib {
namespace index {
AUTIL_LOG_SETUP(ha3, FakePostingMaker);
using namespace std;
using namespace autil;

void FakePostingMaker::parseOnePostingDetail(const string &postingStr,
                                             FakeTextIndexReader::Posting &posting) {
    size_t posBegin = postingStr.find('[');
    string docInfo = postingStr.substr(0, posBegin);
    size_t posEnd = postingStr.find(']', posBegin);
    string posInfo;
    if (posBegin != string::npos && posEnd != string::npos) {
        posInfo = postingStr.substr(posBegin + 1, posEnd - posBegin - 1);
    }
    size_t docPayloadPos = docInfo.find('^');
    posting.docid = StringUtil::fromString<docid_t>(docInfo.substr(0, docPayloadPos));
    if (docPayloadPos != string::npos) {
        posting.docPayload
            = StringUtil::fromString<docpayload_t>(docInfo.substr(docPayloadPos + 1));
    }

    StringTokenizer st(
        posInfo, ",", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        const string &onePos = st[i];
        size_t posPayloadPos = onePos.find('_');
        pos_t position = StringUtil::fromString<pos_t>(onePos.substr(0, posPayloadPos));
        posting.occArray.push_back(position);
        if (posPayloadPos != string::npos) {
            posting.fieldBitArray.push_back(
                StringUtil::fromString<int32_t>(onePos.substr(posPayloadPos + 1)));
        }
    }
}

void FakePostingMaker::makeFakePostingsDetail(const string &str,
                                              FakeTextIndexReader::Map &postingMap) {
    StringTokenizer st(
        str, "\n", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        const string &onePosting = st[i];
        size_t termPos = onePosting.find(':');
        assert(termPos != string::npos);
        string termInfo = onePosting.substr(0, termPos);
        size_t payloadPos = termInfo.find('^');
        string term = termInfo.substr(0, payloadPos);
        termpayload_t termPayload = 0;
        if (payloadPos != string::npos) {
            string payloadStr = termInfo.substr(payloadPos + 1);
            termPayload = (termpayload_t)atoi(payloadStr.c_str());
        }
        string postingsStr = onePosting.substr(termPos + 1);

        StringTokenizer st1(
            postingsStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        FakeTextIndexReader::Postings pts;
        for (size_t j = 0; j < st1.getNumTokens(); ++j) {
            const string &postingStr = st1[j];
            FakeTextIndexReader::Posting pt;
            parseOnePostingDetail(postingStr, pt);
            pts.push_back(pt);
        }
        postingMap[term] = std::make_pair(termPayload, pts);
    }
}

void FakePostingMaker::makeFakePostingsSection(const std::string &str,
                                               FakeTextIndexReader::Map &postingMap) {
    size_t startPos = 0;
    while (true) {
        size_t endPos = str.find('\n', startPos);

        // find a postings line
        string line = str.substr(startPos, endPos - startPos);
        if (line.empty()) {
            break;
        }
        size_t termPos = line.find(':');
        if (termPos == string::npos) {
            break;
        }
        string term = line.substr(0, termPos);
        string postingsStr = line.substr(termPos + 1);
        size_t docPos = 0;

        FakeTextIndexReader::Postings pts;
        // parse postings to each posting
        while ((docPos = postingsStr.find(';')) != string::npos) {
            string postingStr = postingsStr.substr(0, docPos);
            FakeTextIndexReader::Posting pt;
            postingsStr = postingsStr.substr(docPos + 1);
            parseOnePostingSection(postingStr, pt);
            pts.push_back(pt);
        }
        // postingMap[term] = pts;
        // we make a fake pair just for integreting fakePostings and fakePostingsDetail
        postingMap[term] = std::make_pair((uint16_t)0, pts);

        if (string::npos == endPos) {
            break;
        } else {
            startPos = endPos + 1;
        }
    }
}

void FakePostingMaker::parseOnePostingSection(const std::string &postingStr,
                                              FakeTextIndexReader::Posting &posting) {
    size_t occsPos = postingStr.find('[');
    string docStr = postingStr.substr(0, occsPos);
    posting.docid = atoi(docStr.c_str());

    if (occsPos == string::npos) {
        return;
    }
    size_t tmpPos = postingStr.find(']');
    string occsStr = postingStr.substr(occsPos + 1, tmpPos - occsPos - 1);

    posting.occArray.clear();
    size_t occPos = 0;
    while ((occPos = occsStr.find(',')) != string::npos) {
        string occStr = occsStr.substr(0, occPos);
        occsStr = occsStr.substr(occPos + 1);
        pos_t occ = atoi(occStr.c_str());
        size_t pos = occStr.find('_');
        pos_t field = 0;
        if (pos != string::npos) {
            size_t posSecId = occStr.find('_', pos + 1);
            string fieldStr = occStr.substr(pos + 1, posSecId - pos - 1);
            string sectionStr = occStr.substr(posSecId + 1);
            occ = atoi(occStr.substr(0, pos).c_str());
            field = atoi(fieldStr.c_str());
            uint32_t sectionId = atoi(sectionStr.c_str());
            posting.fieldBitArray.push_back(field);
            posting.sectionIdArray.push_back(sectionId);
        }
        posting.occArray.push_back(occ);
    }
    size_t lastPos = occsStr.find('_');
    pos_t lastocc = atoi(occsStr.substr(0).c_str());
    pos_t lastField = 0;
    if (lastPos != string::npos) {
        size_t posSecId = occsStr.find('_', lastPos + 1);
        string lastFieldStr = occsStr.substr(lastPos + 1, posSecId - lastPos - 1);
        string sectionStr = occsStr.substr(posSecId + 1);
        lastocc = atoi(occsStr.substr(0, lastPos).c_str());
        lastField = atoi(lastFieldStr.c_str());
        uint32_t sectionId = atoi(sectionStr.c_str());
        posting.fieldBitArray.push_back(lastField);
        posting.sectionIdArray.push_back(sectionId);
    }
    posting.occArray.push_back(lastocc);
}

} // namespace index
} // namespace indexlib
