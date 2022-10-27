#include "indexlib/misc/regular_expression.h"

using namespace std;

IE_NAMESPACE_BEGIN(misc);
IE_LOG_SETUP(misc, RegularExpression);

RegularExpression::RegularExpression()
    : mLatestErrorCode(0)
    , mInitialized(false)
{
}

RegularExpression::~RegularExpression() 
{
    if (mInitialized)
    {
        regfree(&mRegex);
    }
}


bool RegularExpression::Init(const string &pattern)
{
    if (mInitialized)
    {
        return false;
    }

    mInitialized = true;
    mLatestErrorCode = regcomp(&mRegex, pattern.c_str(), REG_EXTENDED | REG_NOSUB);
    return mLatestErrorCode == 0;
}

bool RegularExpression::Match(const string &string) const
{
    if (unlikely(!mInitialized))
    {
        return false;
    }

    mLatestErrorCode = regexec(&mRegex, string.c_str(), 0, NULL, 0);
    return mLatestErrorCode == 0;
}

std::string RegularExpression::GetLatestErrMessage() const
{
    if (!mInitialized)
    {
        return std::string("Not Call Init");
    }
    static char errorMessage[1024] = {0};
    size_t len = regerror(mLatestErrorCode, &mRegex,
                          errorMessage, sizeof(errorMessage));
    len = len < sizeof(errorMessage) ? len : sizeof(errorMessage) - 1;
    errorMessage[len] = '\0';
    return std::string(errorMessage);
}

IE_NAMESPACE_END(misc);

