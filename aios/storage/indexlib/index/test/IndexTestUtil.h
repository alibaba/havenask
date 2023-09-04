#pragma once

#include <memory>
#include <string>

#include "autil/Log.h"
#include "indexlib/base/Types.h"

namespace indexlibv2 { namespace index {

class IndexTestUtil
{
public:
    typedef bool (*ToDelete)(docid_t);

    enum DeletionMode { DM_NODELETE = 0, DM_DELETE_EVEN, DM_DELETE_SOME, DM_DELETE_MANY, DM_DELETE_ALL, DM_MAX_MODE };

public:
    static bool NoDelete(docid_t docId) { return false; }

    static bool DeleteEven(docid_t docId) { return docId % 2 == 0; }

    static bool DeleteSome(docid_t docId) { return (docId % 7 == 3) || (docId % 11 == 5) || (docId % 13 == 7); }

    static bool DeleteMany(docid_t docId)
    {
        docid_t tmpDocId = docId % 16;
        return (tmpDocId < 10);
    }

    static bool DeleteAll(docid_t docId) { return true; }

    static void ResetDir(const std::string& dir);
    static void MkDir(const std::string& dir);
    static void CleanDir(const std::string& dir);

public:
    static ToDelete deleteFuncs[DM_MAX_MODE];

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////
}} // namespace indexlibv2::index
