#ifndef __BUILD_SERVICE_TASKS_UNITTEST_H
#define __BUILD_SERVICE_TASKS_UNITTEST_H

#include <dlfcn.h>
#include <execinfo.h>
#include <string>
#include <typeinfo>
#include <unistd.h>
#define GTEST_USE_OWN_TR1_TUPLE 0
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "build_service/util/Log.h"
#include "fslib/util/FileUtil.h"
#include "unittest/unittest.h"

#define UNIT_TEST UT

using testing::_;
using testing::DoAll;
using testing::Invoke;
using testing::Return;
using testing::SetArgReferee;
using testing::Throw;

typedef TESTBASE BUILD_SERVICE_TASKS_TESTBASE;

AUTIL_DECLARE_AND_SETUP_LOGGER(build_service_tasks, test);

#endif
