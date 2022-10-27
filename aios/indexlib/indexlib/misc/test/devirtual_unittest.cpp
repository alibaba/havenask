#include "indexlib/misc/test/devirtual_unittest.h"
#include "indexlib/misc/test/base_class.h"
#include "indexlib/misc/test/childA_class.h"
#include "indexlib/misc/test/childB_class.h"
#include "indexlib/misc/test/base_class.hpp"

using namespace std;

IE_NAMESPACE_BEGIN(misc);
IE_LOG_SETUP(misc, DeVirtualTest);

DeVirtualTest::DeVirtualTest()
{
}

DeVirtualTest::~DeVirtualTest()
{
}

void DeVirtualTest::CaseSetUp()
{
}

void DeVirtualTest::CaseTearDown()
{
}

void DeVirtualTest::TestSimpleProcess()
{
    BaseClass* ca = new ChildClassA();
    BaseClass* cb = new ChildClassB();

    // call virtual func
    ca->print();
    cb->print();

    // inner switch case
    ca->printImpl();
    cb->printImpl();

    delete ca;
    delete cb;
}

IE_NAMESPACE_END(misc);

