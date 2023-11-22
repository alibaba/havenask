#pragma once

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KvFactoryTest : public INDEXLIB_TESTBASE
{
public:
    KvFactoryTest();
    ~KvFactoryTest();

    DECLARE_CLASS_NAME(KvFactoryTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
