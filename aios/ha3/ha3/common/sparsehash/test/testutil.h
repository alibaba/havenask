// Copyright (c) 2010, Google Inc.
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
// 
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// ---

// This macro mimics a unittest framework, but is a bit less flexible
// than most.  It requires a superclass to derive from, and does all
// work in global constructors.  The tricky part is implementing
// TYPED_TEST.

#ifndef SPARSEHASH_TEST_UTIL_H_
#define SPARSEHASH_TEST_UTIL_H_

#include <ha3/common/sparsehash/internal/sparseconfig.h>
#include <ha3/common/sparsehash/config.h>
#include <stdio.h>
#include <stdlib.h>    // for exit
#include <stdexcept>   // for length_error

_START_GOOGLE_NAMESPACE_

namespace testing {

#define EXPECT_TRUE(cond)  do {                         \
  if (!(cond)) {                                        \
    ::fputs("Test failed: " #cond "\n", stderr);        \
    ::exit(1);                                          \
  }                                                     \
} while (0)

#define EXPECT_FALSE(a)  EXPECT_TRUE(!(a))
#define EXPECT_EQ(a, b)  EXPECT_TRUE((a) == (b))
#define EXPECT_NE(a, b)  EXPECT_TRUE((a) != (b))
#define EXPECT_LT(a, b)  EXPECT_TRUE((a) < (b))
#define EXPECT_GT(a, b)  EXPECT_TRUE((a) > (b))
#define EXPECT_LE(a, b)  EXPECT_TRUE((a) <= (b))
#define EXPECT_GE(a, b)  EXPECT_TRUE((a) >= (b))

#define EXPECT_DEATH(cmd, expected_error_string)                          \
  try {                                                                   \
    cmd;                                                                  \
    EXPECT_FALSE("did not see expected error: " #expected_error_string);  \
  } catch (const std::length_error&) {                                    \
    /* Good, the cmd failed. */                                           \
  }

#define TEST(suitename, testname)                                       \
  class TEST_##suitename##_##testname {                                 \
   public:                                                              \
    TEST_##suitename##_##testname() {                                   \
      ::fputs("Running " #suitename "." #testname "\n", stderr);        \
      Run();                                                            \
    }                                                                   \
    void Run();                                                         \
  };                                                                    \
  static TEST_##suitename##_##testname                                  \
      test_instance_##suitename##_##testname;                           \
  void TEST_##suitename##_##testname::Run()


template<typename C1> struct TypeList1 {
  typedef C1 type1;
};

// I need to list 18 types here, for code below to compile, though
// only the first 6 are ever used.
#define TYPED_TEST_CASE_1(classname, typelist)  \
  typedef typelist::type1 classname##_type1;    \
  typedef typelist::type1 classname##_type2;    \
  typedef typelist::type1 classname##_type3;    \
  static const int classname##_numtypes = 1;   

template<typename C1, typename C2, typename C3> struct TypeList3 {
  typedef C1 type1;
  typedef C2 type2;
  typedef C3 type3;
};

#define TYPED_TEST_CASE_3(classname, typelist)  \
  typedef typelist::type1 classname##_type1;    \
  typedef typelist::type2 classname##_type2;    \
  typedef typelist::type3 classname##_type3;    \
  static const int classname##_numtypes = 3;

#define TYPED_TEST(superclass, testname)                                \
  template<typename TypeParam>                                          \
  class TEST_onetype_##superclass##_##testname :                        \
      public superclass<TypeParam> {                                    \
   public:                                                              \
    TEST_onetype_##superclass##_##testname() {                          \
      Run();                                                            \
    }                                                                   \
   private:                                                             \
    void Run();                                                         \
  };                                                                    \
  class TEST_typed_##superclass##_##testname {                          \
   public:                                                              \
    explicit TEST_typed_##superclass##_##testname() {                   \
      if (superclass##_numtypes >= 1) {                                 \
        ::fputs("Running " #superclass "." #testname ".1\n", stderr);   \
        TEST_onetype_##superclass##_##testname<superclass##_type1> t;   \
      }                                                                 \
      if (superclass##_numtypes >= 2) {                                 \
        ::fputs("Running " #superclass "." #testname ".2\n", stderr);   \
        TEST_onetype_##superclass##_##testname<superclass##_type2> t;   \
      }                                                                 \
      if (superclass##_numtypes >= 3) {                                 \
        ::fputs("Running " #superclass "." #testname ".3\n", stderr);   \
        TEST_onetype_##superclass##_##testname<superclass##_type3> t;   \
      }                                                                 \
    }                                                                   \
  };                                                                    \
  static TEST_typed_##superclass##_##testname                           \
      test_instance_typed_##superclass##_##testname;                    \
  template<class TypeParam>                                             \
  void TEST_onetype_##superclass##_##testname<TypeParam>::Run()

// This is a dummy class just to make converting from internal-google
// to opensourcing easier.
class Test { };

} // namespace testing

_END_GOOGLE_NAMESPACE_

#endif  // SPARSEHASH_TEST_UTIL_H_
