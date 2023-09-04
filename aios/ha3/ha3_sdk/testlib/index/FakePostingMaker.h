#pragma once

#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"

namespace indexlib {
namespace index {

class FakePostingMaker {
public:
    /*
     * term^termpayload:docid^docpayload[position_pospayload,position_pospayload,...]
     */
    static void makeFakePostingsDetail(const std::string &str,
                                       FakeTextIndexReader::Map &postingMap);
    static void makeFakePostingsSection(const std::string &str,
                                        FakeTextIndexReader::Map &postingMap);

protected:
    static void parseOnePostingDetail(const std::string &postingStr,
                                      FakeTextIndexReader::Posting &posting);

    static void parseOnePostingSection(const std::string &postingStr,
                                       FakeTextIndexReader::Posting &posting);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace index
} // namespace indexlib
