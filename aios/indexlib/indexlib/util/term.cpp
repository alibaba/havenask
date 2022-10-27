#include "indexlib/util/term.h"
#include <sstream>

using namespace std;

IE_NAMESPACE_BEGIN(util);


bool Term::operator == (const Term& term) const
{
    if (&term == this)
    {
        return true;
    }

    if (mWord != term.mWord || mIndexName != term.mIndexName)
    {
        return false;
    }
    return mTruncateName == term.mTruncateName && mLiteTerm == term.mLiteTerm;
}

std::string Term::ToString() const
{
    std::stringstream ss;
    ss << "Term:[" << mIndexName << "," << mTruncateName << "," << mWord;
    const LiteTerm* liteTerm = GetLiteTerm();
    if (liteTerm)
    {
        ss << " | LiteTerm:" << liteTerm->GetIndexId() << ","
           << liteTerm->GetTruncateIndexId()
           << "," << liteTerm->GetTermHashKey();
    }
    ss << "]";
    return ss.str();
}

std::ostream& operator <<(std::ostream &os, const Term& term)
{
    return os << term.ToString();
}

void Term::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("word", mWord);
    json.Jsonize("indexName", mIndexName);
    json.Jsonize("truncateName", mTruncateName);
}

IE_NAMESPACE_END(util);

