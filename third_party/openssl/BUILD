load('//bazel:defs.bzl', 'rpm_library')
hdrs = [
    'openssl/aes.h', 'openssl/asn1.h', 'openssl/asn1_mac.h', 'openssl/asn1t.h',
    'openssl/bio.h', 'openssl/blowfish.h', 'openssl/bn.h', 'openssl/buffer.h',
    'openssl/camellia.h', 'openssl/cast.h', 'openssl/cmac.h', 'openssl/cms.h',
    'openssl/comp.h', 'openssl/conf.h', 'openssl/conf_api.h',
    'openssl/crypto.h', 'openssl/des.h', 'openssl/des_old.h', 'openssl/dh.h',
    'openssl/dsa.h', 'openssl/dso.h', 'openssl/dtls1.h', 'openssl/e_os2.h',
    'openssl/ebcdic.h', 'openssl/ec.h', 'openssl/ecdh.h', 'openssl/ecdsa.h',
    'openssl/engine.h', 'openssl/err.h', 'openssl/evp.h', 'openssl/fips.h',
    'openssl/fips_rand.h', 'openssl/hmac.h', 'openssl/idea.h',
    'openssl/krb5_asn.h', 'openssl/kssl.h', 'openssl/lhash.h', 'openssl/md2.h',
    'openssl/md4.h', 'openssl/md5.h', 'openssl/modes.h', 'openssl/obj_mac.h',
    'openssl/objects.h', 'openssl/ocsp.h', 'openssl/opensslconf-x86_64.h',
    'openssl/opensslconf.h', 'openssl/opensslv.h', 'openssl/ossl_typ.h',
    'openssl/pem.h', 'openssl/pem2.h', 'openssl/pkcs12.h', 'openssl/pkcs7.h',
    'openssl/pqueue.h', 'openssl/rand.h', 'openssl/rc2.h', 'openssl/rc4.h',
    'openssl/rc5.h', 'openssl/ripemd.h', 'openssl/rsa.h', 'openssl/safestack.h',
    'openssl/seed.h', 'openssl/sha.h', 'openssl/srtp.h', 'openssl/ssl.h',
    'openssl/ssl2.h', 'openssl/ssl23.h', 'openssl/ssl3.h', 'openssl/stack.h',
    'openssl/symhacks.h', 'openssl/tls1.h', 'openssl/ts.h', 'openssl/txt_db.h',
    'openssl/ui.h', 'openssl/ui_compat.h', 'openssl/whrlpool.h',
    'openssl/x509.h', 'openssl/x509_vfy.h', 'openssl/x509v3.h', 'krb5.h',
    'krb5/ccselect_plugin.h', 'krb5/hostrealm_plugin.h',
    'krb5/kdcpreauth_plugin.h', 'krb5/locate_plugin.h', 'krb5/pwqual_plugin.h',
    'krb5/certauth_plugin.h', 'krb5/kadm5_hook_plugin.h', 'krb5/krb5.h',
    'krb5/plugin.h', 'krb5/clpreauth_plugin.h', 'krb5/kdcpolicy_plugin.h',
    'krb5/localauth_plugin.h', 'krb5/preauth_plugin.h', 'et/com_err.h'
]
rpm_library(
    name='ssl_headers',
    rpms=[
        '@openssl//file:file', '@openssl-devel//file:file',
        '@openssl-libs//file:file', '@krb5-devel//file:file',
        '@libcom_err-devel//file:file'
    ],
    hdrs=hdrs,
    include_path='usr/include/',
    lib_path='',
    include_prefix=''
)
rpm_library(
    name='ssl',
    rpms=[
        '@openssl//file:file', '@openssl-devel//file:file',
        '@openssl-libs//file:file'
    ],
    hdrs=[],
    shared_lib='libssl.so',
    include_path='usr/include/',
    lib_path='usr/lib64',
    include_prefix='',
    deps=[':ssl_headers']
)
rpm_library(
    name='crypto',
    rpms=[
        '@openssl//file:file', '@openssl-devel//file:file',
        '@openssl-libs//file:file'
    ],
    hdrs=[],
    shared_lib='libcrypto.so',
    include_path='usr/include/',
    lib_path='usr/lib64',
    include_prefix='',
    deps=[':ssl_headers']
)
