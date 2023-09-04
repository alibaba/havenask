#include "indexlib/test/docid_query.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, DocidQuery);

DocidQuery::DocidQuery() : Query(qt_docid), mCursor(0) {}

DocidQuery::~DocidQuery() {}

bool DocidQuery::Init(const std::string& docidStr)
{
    vector<docid_t> docidRange;
    StringUtil::fromString(docidStr, docidRange, "-");
    if (docidRange.size() == 1) {
        mDocIdVec.push_back(docidRange[0]);
        return true;
    }
    if (docidRange.size() == 2) {
        // [begin, end]
        for (docid_t docid = docidRange[0]; docid <= docidRange[1]; ++docid) {
            mDocIdVec.push_back(docid);
        }
        return true;
    }
    return false;
}

docid_t DocidQuery::Seek(docid_t docid)
{
    while (mCursor < mDocIdVec.size()) {
        docid_t retDocId = mDocIdVec[mCursor++];
        if (retDocId >= docid) {
            return retDocId;
        }
    }
    return INVALID_DOCID;
}
}} // namespace indexlib::test
