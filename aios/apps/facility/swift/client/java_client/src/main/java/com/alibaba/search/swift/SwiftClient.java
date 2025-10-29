package com.alibaba.search.swift;

import com.alibaba.search.swift.protocol.ErrCode;
import com.alibaba.search.swift.library.SwiftClientApiLibrary;
import com.alibaba.search.swift.exception.*;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.ptr.NativeLongByReference;
import java.util.*;
import java.io.*;

public class SwiftClient {
    private NativeLong _clientPtr;
    private static boolean _isExtracted = false;
    private List<SwiftWriter> _writerList = new LinkedList<SwiftWriter>();
    private List<SwiftReader> _readerList = new LinkedList<SwiftReader>();
    private List<SwiftAdminAdaptor> _adapterList = new LinkedList<SwiftAdminAdaptor>();
    
    public SwiftClient() {
        _clientPtr = new NativeLong(0);
    }

    public synchronized void init(String clientConfigStr) throws SwiftException {
        if (_clientPtr.longValue() != 0) {
            return;
        }
        String jnaLibPath = System.getProperty("jna.tmpdir");
        if (jnaLibPath == null || jnaLibPath.isEmpty()) {
            jnaLibPath = System.getProperty("user.dir");
        }
        System.setProperty("jna.library.path", jnaLibPath);
        String[] libs = new String[]{
                "libprotobuf.so.7.0.0",
                "libzookeeper_mt.so.2.0.0",
                "libalog.so.13.2.2",
                "libanet.so.13.2.1",
                "libarpc.so.13.2.2",
                "libautil.so.9.3",
                "libswift_client_minimal.so.107.2",
                "swift_alog.conf"};
        if (!extractBinary(libs, jnaLibPath)) {
            throw new SwiftException();
        }
        NativeLongByReference longRef = new NativeLongByReference();
        int retEc = SwiftClientApiLibrary.createSwiftClient(clientConfigStr, longRef);

        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        if (ec != ErrCode.ErrorCode.ERROR_NONE) {
            throw new SwiftException(ec);
        }
        _clientPtr = longRef.getValue();
    }

    public synchronized SwiftReader createReader(String readerConfigStr) throws SwiftException {
        if (_clientPtr.longValue() == 0) {
            throw new SwiftException();
        }
        NativeLongByReference longRef = new NativeLongByReference();
        int retEc = SwiftClientApiLibrary.createSwiftReader(_clientPtr, readerConfigStr, longRef);
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        NativeLong readerPtr = longRef.getValue();
        if (ec != ErrCode.ErrorCode.ERROR_NONE || readerPtr.longValue() == 0) {
            throw new SwiftException(ec);
        }
        SwiftReader reader = new SwiftReader(readerPtr);
        _readerList.add(reader);
        return reader;
    }

    public synchronized SwiftWriter createWriter(String writerConfigStr) throws SwiftException {
        if (_clientPtr.longValue() == 0) {
            throw new SwiftException();
        }
        NativeLongByReference longRef = new NativeLongByReference();
        int retEc = SwiftClientApiLibrary.createSwiftWriter(_clientPtr, writerConfigStr, longRef);
        ErrCode.ErrorCode ec = ErrCode.ErrorCode.valueOf(retEc);
        NativeLong writerPtr = longRef.getValue();
        if (ec != ErrCode.ErrorCode.ERROR_NONE || writerPtr.longValue() == 0) {
            throw new SwiftException(ec);
        }
        SwiftWriter writer = new SwiftWriter(writerPtr);
        _writerList.add(writer);
        return writer;
    }

    public synchronized SwiftAdminAdaptor getAdminAdapter() throws SwiftException {
        if (_clientPtr.longValue() == 0) {
            throw new SwiftException();
        }
        NativeLong adminPtr = SwiftClientApiLibrary.getAdminAdapter(_clientPtr);
        SwiftAdminAdaptor adaptor = new SwiftAdminAdaptor(adminPtr);
        _adapterList.add(adaptor);
        return adaptor;
    }

    public synchronized SwiftAdminAdaptor getAdminAdapter(String zkPath) throws SwiftException {
        if (_clientPtr.longValue() == 0) {
            throw new SwiftException();
        }
        NativeLong adminPtr = SwiftClientApiLibrary.getAdminAdapterByZk(_clientPtr, zkPath);
        SwiftAdminAdaptor adaptor = new SwiftAdminAdaptor(adminPtr);
        _adapterList.add(adaptor);
        return adaptor;
    }

    public synchronized boolean isClosed() {
        return _clientPtr.longValue() == 0;
    }
    
    public synchronized void close() {
        if (_clientPtr.longValue() == 0) {
            return;
        }
        Iterator<SwiftWriter> writerIter = _writerList.iterator();
        while (writerIter.hasNext()) {
            SwiftWriter writer = writerIter.next();
            writer.close();
        }
        _writerList.clear();
        Iterator<SwiftReader> readerIter = _readerList.iterator();
        while (readerIter.hasNext()) {
            SwiftReader reader = readerIter.next();
            reader.close();
        }
        _readerList.clear();
        Iterator<SwiftAdminAdaptor> adaptorIter = _adapterList.iterator();
        while (adaptorIter.hasNext()) {
            SwiftAdminAdaptor adaptor = adaptorIter.next();
            adaptor.close();
        }
        _adapterList.clear();

        SwiftClientApiLibrary.deleteSwiftClient(_clientPtr);
        _clientPtr.setValue(0);
    }

    private static synchronized boolean extractBinary(String[] libs, String jnaLibPath) {
        if (_isExtracted) {
            return true;
        }
        for (int i = 0; i < libs.length; i++) {
            InputStream is = SwiftClient.class.getResourceAsStream("/linux-x86-64/" + libs[i]);
            if (is == null) {
                System.err.println("File " + libs[i] + " not found");
                return false;
            }
            byte[] buffer = new byte[8192];
            OutputStream os = null;
            try {

                File unpackedFile = new File(jnaLibPath + "/" + libs[i]);
                unpackedFile.deleteOnExit();
                if (unpackedFile.exists()) {
                    continue;
                }
                os = new FileOutputStream(unpackedFile);
                int read;
                while ((read = is.read(buffer)) != -1) {
                    os.write(buffer, 0, read);
                }
                unpackedFile.setExecutable(true, false);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            } finally {
                try {
                    is.close();
                } catch (IOException ignore) {
                    System.err.println(ignore.toString());
                }
                if (os != null) {
                    try {
                        os.close();
                    } catch (IOException ignore) {
                        System.err.println(ignore.toString());
                    }
                }
            }
        }
        _isExtracted = true;
        return true;
    }

    protected void finalize() {
        close();
    }
}
