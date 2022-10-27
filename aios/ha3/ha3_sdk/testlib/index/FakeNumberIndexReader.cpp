#include <ha3_sdk/testlib/index/FakeNumberIndexReader.h>
#include <autil/StringUtil.h>
#include <ha3/util/NumericLimits.h>
#include <indexlib/common/number_term.h>
#include <ha3_sdk/testlib/index/FakeNumberPostingIterator.h>
using namespace std;
using namespace autil;

IE_NAMESPACE_USE(common);
USE_HA3_NAMESPACE(util);
IE_NAMESPACE_BEGIN(index);
HA3_LOG_SETUP(index, FakeNumberIndexReader);

const char *LINE_DELIMITER = ";";
const char *VALUE_DELIMITER = ":";
const char *DOCID_DELIMITER = ",";


PostingIterator *FakeNumberIndexReader::Lookup(const common::Term& term, uint32_t statePoolSize, 
        PostingType type, autil::mem_pool::Pool *pool) 
{
    const Int32Term &int32Term = dynamic_cast<const Int32Term&> (term);
    FakeNumberPostingIterator *fkNumbPostIt = POOL_COMPATIBLE_NEW_CLASS(pool,
            FakeNumberPostingIterator, _numberValuePtr,
            int32Term.GetLeftNumber(), int32Term.GetRightNumber(), pool);
    return fkNumbPostIt;
}

void FakeNumberIndexReader::setNumberValuesFromString(const string &numberValues) {
    vector<string> lines = StringUtil::split(numberValues, LINE_DELIMITER);
    
    for (vector<string>::iterator lineIterator = lines.begin();
         lineIterator != lines.end(); lineIterator++) 
    {
        parseOneLine(*lineIterator);
    }
}

FakeNumberIndexReader::NumberValuesPtr FakeNumberIndexReader::getNumberValues() {
    return _numberValuePtr;
}

void FakeNumberIndexReader::clear() {
    _numberValuePtr->clear();
}

size_t FakeNumberIndexReader::size() {
    return _numberValuePtr->size();
}

// parse a string like "3: 2, 4, 7" where 3 is number value, 2, 4, 7 are docids
void FakeNumberIndexReader::parseOneLine(const string &aLine) {
    vector<string> ss = StringUtil::split(aLine, VALUE_DELIMITER);
    if (ss.size() != 2) { // not a valid string
        return;
    }

    StringUtil::trim(ss[0]);
    int32_t value;
    StringUtil::strToInt32(ss[0].c_str(), value);

    // parse all docids and setup _numberValuePtr accordingly
    vector<string> docIdStrings = StringUtil::split(ss[1], DOCID_DELIMITER);
    for (vector<string>::iterator docIdIteraotr = docIdStrings.begin();
         docIdIteraotr != docIdStrings.end(); docIdIteraotr++)
    {
        int32_t docid;
        StringUtil::trim(*docIdIteraotr);
        if (!StringUtil::strToInt32(docIdIteraotr->c_str(), docid)) {
            continue;
        }
        if ((size_t)docid >= _numberValuePtr->size()) {
            _numberValuePtr->reserve(docid * 2); // just for performance concern
            _numberValuePtr->resize(docid + 1, HA3_NS(util)::NumericLimits<int32_t>::min());
        }
        _numberValuePtr->at(docid) = value;
    }
}

const SectionAttributeReader* FakeNumberIndexReader::GetSectionReader(const std::string& indexName) const {
    assert(false);
    return NULL;
}

IE_NAMESPACE_END(index);

