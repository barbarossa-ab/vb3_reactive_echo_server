/* -------------------------------------------------------------------------- *
 * P3_ReactiveEchoServer
 * 
 * This file contains an implementation of an echo server - a program 
 * that accepts connections and echoes back whatever you write to it.
 * The implementation uses a Reactor design pattern, with the following roles :
 *  - EchoInitDispatch : initiation dispatcher, uses a selector to listen
 *      to multiple channels (handles) and forward events to the appropiate 
 *      event handlers; 
 *  - EventHandler : generic specification of an event handler;
 *  - EchoServerAcceptor : implementation of EventHandler, handles connection 
 *      requests from clients
 *  - EchoServerHandler : implementation of EventHandler, handles messages 
 *      from clients - echoes back
 * 
 * Wrapper facade is used to encapsulate low-level IO : 
 *  - DataSocket - incoming data
 *  - ListeningSocket - incoming connection requests
 * 
 * The solution also uses some helper classes : 
 *  - Event : for event definition and related constants
 *  - EventProcStatus : return type for event processing function
 *  - Log : debugging purposes
 * 
 * Author : Barbarossa
 * -------------------------------------------------------------------------- */
package p3_reactiveechoserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;


public class P3_ReactiveEchoServer {

    public static final int ECHO_SERVER_PORT = 29999;

    public static void main(String[] args) {
        EchoServerAcceptor ac = new EchoServerAcceptor(ECHO_SERVER_PORT);

        try {
            EchoInitDispatch.getInstance().registerHandler(ac, Event.EV_ACCEPT);
            while (true) {
                EchoInitDispatch.getInstance().handleEvents();
            }
        } catch (IOException ex) {
            Log.println(ex.getStackTrace().toString());
        }

    }
    
}

/* -------------------------------------------------------------------------- */
/* Reactor pattern classes */

/** 
 *  The initiation dispatcher :
 *      - listens to multiple channels using a selector (demux function)
 *      - manages event handlers (register, unregister)
 *      - at event occurrence, forwards the event to the appropriate handler
 */
class EchoInitDispatch {

    private Selector selector;
    private static EchoInitDispatch instance = null;
    private HashMap <SelectionKey, EventHandler> handlers;
    
    private EchoInitDispatch() {
        try {
            selector = SelectorProvider.provider().openSelector();
        } catch (IOException ex) {
            Log.println(ex.getStackTrace().toString());
        }
        handlers = new HashMap <> ();

        Log.println("EchoInitDispatch created...");
    }

    public static EchoInitDispatch getInstance() {
        if (instance == null) {
            instance = new EchoInitDispatch();
        }
        return instance;
    }

    public void handleEvents() throws IOException {
        Log.println("------------------------------------------------------------");
        Log.println("EchoInitDispatch is waiting for events on selector...");
        selector.select();

        Log.println("    Events received...");
        Set readyKeys = selector.selectedKeys();
        Iterator iterator = readyKeys.iterator();

        while (iterator.hasNext()) {
            SelectionKey key = (SelectionKey) iterator.next();
            iterator.remove();

            if (!key.isValid()) {
                continue;
            }
            if (key.isAcceptable()) {
                Log.println("    ACCEPT event on key " + key);
                callHandler(key, Event.EV_ACCEPT);
            } else if (key.isReadable()) {
                Log.println("    READ event on key " + key);
                callHandler(key, Event.EV_READ);
            }
        }
    }
    

    public void callHandler(SelectionKey key, int ev) {
            boolean unregister = false;
            EventHandler h = handlers.get(key);
            
            try {
                EventProcStatus evStatus = h.handleEvent(ev);
                switch(evStatus) {
                    case EV_PROC_CLIENT_DISCONNECTED :
                        unregister = true;
                        break;
                }
            } catch (Exception ex) {
                Log.println("    Exception processing event " + ev
                        + " from key " + key + " : " + ex.getStackTrace());
                unregister = true;
            }

            if(unregister) {
                removeHandler(key);
            }
    }

    public SelectionKey registerHandler(EventHandler h, int eventMask) {
        int op = 0;
        SelectionKey key = null;

        if ((eventMask & Event.EV_ACCEPT) != 0) {
            op |= SelectionKey.OP_ACCEPT;
            Log.println("    EventHandler attached for OP_ACCEPT operation...");
        }
        if ((eventMask & Event.EV_READ) != 0) {
            op |= SelectionKey.OP_READ;
            Log.println("    EventHandler attached for OP_READ operation...");
        }
        
        try {
            h.getHandle().configureBlocking(false);
            key = h.getHandle().register(selector, op);
            handlers.put(key, h);
        } catch (IOException ex) {
            Log.println(ex.getStackTrace().toString());
        }
        
        return key;
    }

    public void removeHandler(SelectionKey key) {
        key.cancel();
        handlers.remove(key);
    }
    
}

/** 
 *  EventHandler generic specification
 */
interface EventHandler {

    public EventProcStatus handleEvent(int evType) throws Exception;

    public SelectableChannel getHandle();
}



/** 
 *  The acceptor : 
 *      - accepts & establishes connections from the clients
 *      - registers message listeners with the initiation dispatcher
 */
class EchoServerAcceptor implements EventHandler {

    private ListeningSocket socket;

    public EchoServerAcceptor(int port) {
        try {
            this.socket = new ListeningSocket(port);
        } catch (IOException ex) {
            Log.println(ex.getStackTrace().toString());
        }

        Log.println("        EchoAcceptor created...");
    }

    @Override
    public EventProcStatus handleEvent(int evType) {
        Log.println("        EchoAcceptor reading event...");

        if (evType != Event.EV_ACCEPT) {
            Log.println("        EchoAcceptor did not find EV_ACCEPT...");
            return EventProcStatus.EV_PROC_WRONG_EV;
        }
        
        DataSocket cs = socket.accept();
        if (cs != null) {
            EchoInitDispatch.getInstance().registerHandler(
                    new EchoServerHandler(cs),
                    Event.EV_READ);
        }

        Log.println("        EchoAcceptor processed EV_ACCEPT succesfully.");
        return EventProcStatus.EV_PROC_OK;
    }

    @Override
    public SelectableChannel getHandle() {
        return (SelectableChannel)socket.getHandle();
    }
}


/** 
 *  The message handler : 
 *      - forwards the message back to the client
 */
class EchoServerHandler implements EventHandler {

    private DataSocket socket;
    
    private static int idGen = 1;
    private int id;

    public EchoServerHandler(DataSocket socket) {
        this.socket = socket;
        this.id = idGen++;
        Log.println("        EchoServerHandler created...");
    }

    @Override
    public EventProcStatus handleEvent(int evType) throws IOException {

        String data = this.socket.read();
        if (data == null) {
            Log.println("        EchoServerHandler : client disconnected " + 
                    this.socket.getPeerDescription());
            socket.close();
            return EventProcStatus.EV_PROC_CLIENT_DISCONNECTED;
        }
        
        
        System.out.println("        EchoServerHandler : " + 
                this.socket.getPeerDescription() + " Wrote: " + data);
		this.socket.write(data);

        Log.println("        EchoServerHandler " + id + 
                " succesfully processed message");
        return EventProcStatus.EV_PROC_OK;
    }

    @Override
    public SelectableChannel getHandle() {
        return (SelectableChannel)socket.getHandle();
    }
}

/* -------------------------------------------------------------------------- */
/* Wrapper facade  */

/**
 * Socket for listening to incoming connection requests
 */
class ListeningSocket {

    private ServerSocketChannel socket;

    public ListeningSocket(int port) throws IOException {
        this.socket = ServerSocketChannel.open();
        this.socket.configureBlocking(false);
        this.socket.socket().bind(new InetSocketAddress(port));
    }

    public DataSocket accept() {
        try {
            SocketChannel s = this.socket.accept();
            if (s == null) {
                System.out.println("Failed to accept");
                return null;
            }
            s.configureBlocking(false);
            return new DataSocket(s);
        } catch (IOException e) {
            System.out.println("socket accept failed");
            Log.println(e.getStackTrace().toString());
        }
        return null;
    }

    public SelectableChannel getHandle() {
        return socket;
    }
}

/**
 * Socket for listening to incoming streams of data
 */
class DataSocket {

    private SocketChannel socket;

    public DataSocket(SocketChannel s) {
        this.socket = s;
    }

    public SelectableChannel getHandle() {
        return socket;
    }

    public String read() {
        Charset charset = Charset.forName("ascii");
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try {
            int nread = this.socket.read(buffer);
            if (nread == -1) {
                return null;
            }
            buffer.flip();
            CharBuffer buf = charset.decode(buffer);
            return buf.toString();
        } catch (IOException e) {
            Log.println("Failed to read : " + e.getStackTrace());
            return null;
        }
    }

    public String getPeerDescription() {
        SocketAddress addr = this.socket.socket().getRemoteSocketAddress();
        if (addr != null) {
            return addr.toString();
        }
        return null;
    }

    public void write(String data) {
        Charset charset = Charset.forName("ascii");
        CharBuffer buf = CharBuffer.wrap(data);
        ByteBuffer b = charset.encode(buf);
        try {
            this.socket.write(b);
        } catch (IOException e) {
            Log.println("Write failed : " + e.getStackTrace());
        }
    }

    public void close() {
        try {
            this.socket.close();
        } catch (IOException e) {
            System.out.println("Warning: Failed to close data socket : "
                    + e.getStackTrace());
        }
    }
}


/* -------------------------------------------------------------------------- */
/* Helper classes */

/* Event types and related constants */
class Event {
    public static final int EV_ACCEPT = 1;
    public static final int EV_READ   = (1 << 1);
    
    public static final int MAX_EVENTS = 2;
}

/* Return type for event processing function */
enum EventProcStatus {
    EV_PROC_OK,
    EV_PROC_WRONG_EV,
    EV_PROC_CLIENT_DISCONNECTED
}

/* For debugging purposes */
class Log {
    public static void println(String text) {
        System.out.println(text);
    }
}
