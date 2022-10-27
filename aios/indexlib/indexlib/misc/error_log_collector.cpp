#include "indexlib/misc/error_log_collector.h"

using namespace std;

IE_NAMESPACE_BEGIN(misc);
alog::Logger* ErrorLogCollector::mLogger = alog::Logger::getLogger("ErrorLogCollector");
string ErrorLogCollector::mIdentifier = "";
bool ErrorLogCollector::mUseErrorLogCollector = false;
bool ErrorLogCollector::mEnableTotalErrorCount = false;

atomic_long ErrorLogCollector::mTotalErrorCount(0);

ErrorLogCollector::ErrorLogCollector() 
{
}

ErrorLogCollector::~ErrorLogCollector() 
{
}

IE_NAMESPACE_END(misc);

