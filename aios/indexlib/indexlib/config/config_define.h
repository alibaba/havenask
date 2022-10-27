#ifndef __INDEXLIB_CONFIG_DEFINE_H
#define __INDEXLIB_CONFIG_DEFINE_H

IE_NAMESPACE_BEGIN(config);

#define IE_CONFIG_ASSERT_EQUAL(a, b, msg)                              \
    do                                                                 \
    {                                                                  \
        if ((a) != (b))                                                \
        {                                                              \
            AUTIL_LEGACY_THROW(misc::AssertEqualException, msg);     \
        }                                                              \
    } while(false)

#define IE_CONFIG_ASSERT(a, msg)                                        \
    do                                                                  \
    {                                                                   \
        if (!(a))                                                       \
        {                                                               \
            AUTIL_LEGACY_THROW(misc::AssertEqualException, msg);      \
        }                                                               \
    } while(false)

#define IE_CONFIG_ASSERT_PARAMETER_VALID(a, msg)                        \
    do                                                                  \
    {                                                                   \
        if (!(a))                                                       \
        {                                                               \
            AUTIL_LEGACY_THROW(misc::BadParameterException, msg);     \
        }                                                               \
    } while(false)

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_CONFIG_DEFINE_H
