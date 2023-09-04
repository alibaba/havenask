#ifndef __INDEXLIB_MOCK_PRIMARY_KEY_LOADER_PLAN_H
#define __INDEXLIB_MOCK_PRIMARY_KEY_LOADER_PLAN_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_load_plan.h"
#include "indexlib/test/unittest.h"

using namespace std;
namespace indexlib { namespace index {

class MockPrimaryKeyLoaderPlan : public PrimaryKeyLoadPlan
{
public:
    MockPrimaryKeyLoaderPlan() : PrimaryKeyLoadPlan() {}
    ~MockPrimaryKeyLoaderPlan() {}

    MOCK_METHOD(string, GetTargetFileName, (), (const, override));
    MOCK_METHOD(file_system::DirectoryPtr, GetTargetFileDirectory, (), (const, override));

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MockPrimaryKeyLoaderPlan);
}} // namespace indexlib::index

#endif //__INDEXLIB_MOCK_PRIMARY_KEY_LOADER_PLAN_H
