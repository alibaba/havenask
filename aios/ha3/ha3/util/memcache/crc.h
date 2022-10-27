#ifndef _CRC_H
#define _CRC_H

#ifdef __cplusplus
extern "C" {
#endif

    void init_crc();

    unsigned long long get_crc64(const char *buf, int buflen);
    unsigned int get_crc32(const char *buf, int buflen);

#ifdef __cplusplus
}
#endif
#endif
