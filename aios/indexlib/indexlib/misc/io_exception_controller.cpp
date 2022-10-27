#include "indexlib/misc/io_exception_controller.h"

using namespace std;

IE_NAMESPACE_BEGIN(misc);
IE_LOG_SETUP(misc, IoExceptionController);

volatile bool IoExceptionController::mHasFileIOException = false;

IoExceptionController::IoExceptionController()
{
}

IoExceptionController::~IoExceptionController() 
{
}

IE_NAMESPACE_END(misc);

