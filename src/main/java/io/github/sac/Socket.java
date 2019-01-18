package io.github.sac;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.neovisionaries.ws.client.*;
import io.github.sac.codec.SocketClusterCodec;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by sachin on 13/11/16.
 */

public class Socket extends Emitter {

    private final Logger logger = Logger.getLogger(Socket.class.getName());

    private AtomicInteger counter;
    private String URL;
    private WebSocketFactory factory;
    private ReconnectStrategy strategy;
    private WebSocket ws;
    private BasicListener listener;
    private String AuthToken;
    private HashMap<Long, Object[]> acks;
    private ConcurrentHashMap<String, Channel> channels;
    private WebSocketAdapter adapter;
    private Map<String, String> headers;
    private SocketClusterCodec codec;
    private static final ObjectMapper mapper = new ObjectMapper();

    public Socket(String URL) {
        this.URL = URL;
        factory = new WebSocketFactory().setConnectionTimeout(5000);
        counter = new AtomicInteger(1);
        acks = new HashMap<>();
        channels = new ConcurrentHashMap<>();
        adapter = getAdapter(this);
        headers = new HashMap<>();
        putDefaultHeaders();
    }

    public Socket setCodec(SocketClusterCodec codec) {
        this.codec = codec;
        return this;
    }

    private void sendData(WebSocket websocket, String data) {
        sendData(websocket, new TextNode(data));
    }

    private void sendData(WebSocket websocket, JsonNode data) {
        if (this.codec == null) {
            websocket.sendText(data.toString());
        } else {
            websocket.sendBinary(this.codec.encode(data));
        }
    }

    private void putDefaultHeaders() {
        headers.put("Accept-Encoding", "gzip, deflate, sdch");
        headers.put("Accept-Language", "en-US,en;q=0.8");
        headers.put("Pragma", "no-cache");
        headers.put("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36");
    }

    public Channel createChannel(String name) {
        if (channels.containsKey(name)) {
            return channels.get(name);
        }

        Channel channel = new Channel(name);
        channels.put(name, channel);
        return channel;
    }

    public ConcurrentHashMap<String, Channel> getChannels() {
        return channels;
    }

    public Channel getChannelByName(String name) {
        channels.get(name);
        return null;
    }

    public void seturl(String url) {
        this.URL = url;
    }

    public void setReconnection(ReconnectStrategy strategy) {
        this.strategy = strategy;
    }

    public void setListener(BasicListener listener) {
        this.listener = listener;
    }

    public Logger getLogger() {
        return logger;
    }

    /**
     * used to set up TLS/SSL connection to server for more details visit neovisionaries websocket client
     */

    public WebSocketFactory getFactorySettings() {
        return factory;
    }

    public void setAuthToken(String token) {
        AuthToken = token;
    }

    public WebSocketAdapter getAdapter(final Socket socket) {
        return new WebSocketAdapter() {

            @Override
            public void onConnected(WebSocket websocket, Map<String, List<String>> headers) throws Exception {

                /**
                 * Code for sending handshake
                 */

                counter.set(1);
                if (strategy != null) {
                    strategy.setAttemptsMade(0);
                }

                ObjectNode handshakeObject = mapper.createObjectNode();
                handshakeObject.put("event", "#handshake");

                ObjectNode object = mapper.createObjectNode();
                object.put("authToken", AuthToken);

                handshakeObject.set("data", object);
                handshakeObject.put("cid", counter.getAndIncrement());

                socket.sendData(websocket, handshakeObject);

                listener.onConnected(Socket.this, headers);

                super.onConnected(websocket, headers);
            }

            @Override
            public void onDisconnected(WebSocket websocket, WebSocketFrame serverCloseFrame, WebSocketFrame clientCloseFrame, boolean closedByServer) throws Exception {
                listener.onDisconnected(Socket.this, serverCloseFrame, clientCloseFrame, closedByServer);
                reconnect();
                super.onDisconnected(websocket, serverCloseFrame, clientCloseFrame, closedByServer);
            }

            @Override
            public void onConnectError(WebSocket websocket, WebSocketException exception) throws Exception {
                listener.onConnectError(Socket.this, exception);
                reconnect();
                super.onConnectError(websocket, exception);
            }

            private Integer getInt(JsonNode node, String fieldName) {
                if (node.has(fieldName)) {
                    JsonNode value = node.get(fieldName);
                    if (!value.isNull() && value.isInt()) {
                        return value.asInt();
                    }
                }
                return null;
            }

            private String getStr(JsonNode node, String fieldName) {
                if (node.has(fieldName)) {
                    JsonNode value = node.get(fieldName);
                    if (!value.isNull() && value.isTextual()) {
                        return value.asText();
                    }
                }
                return null;
            }

            private Boolean getBool(JsonNode node, String fieldName) {
                if (node.has(fieldName)) {
                    JsonNode value = node.get(fieldName);
                    if (!value.isNull() && value.isBoolean()) {
                        return value.asBoolean();
                    }
                }
                return null;
            }


            @Override
            public void onFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {
                JsonNode object;
                if (codec == null) {
                    object = getTextPayload(frame.getPayloadText());
                } else {
                    object = codec.decode(frame.getPayload());
                }

                if (object.isTextual() && object.asText().equalsIgnoreCase("#1")) {
                    /**
                     *  PING-PONG logic goes here
                     */
                    socket.sendData(websocket, "#2");
                } else {
                    /**
                     * Message retrieval mechanism goes here
                     */

                    logger.info("Message :" + object.toString());
                    try {
                        JsonNode dataobject = object.get("data");
                        Integer rid = getInt(object, "rid");
                        Integer cid = getInt(object, "cid");
                        String event = getStr(object, "event");

                        switch (Parser.parse(dataobject, event)) {
                            case ISAUTHENTICATED:
                                listener.onAuthentication(Socket.this, getBool(dataobject, "isAuthenticated"));
                                subscribeChannels();
                                break;
                            case PUBLISH:
                                Socket.this.handlePublish(getStr(dataobject, "channel"), dataobject.get("data"));
                                break;
                            case REMOVETOKEN:
                                setAuthToken(null);
                                break;
                            case SETTOKEN:
                                String token = getStr(dataobject, "token");
                                setAuthToken(token);
                                listener.onSetAuthToken(token, Socket.this);
                                break;
                            case EVENT:
                                if (hasEventAck(event)) {
                                    handleEmitAck(event, dataobject, ack(Long.valueOf(cid)));
                                } else {
                                    Socket.this.handleEmit(event, dataobject);

                                }
                                break;
                            case ACKRECEIVE:
                                if (acks.containsKey((long) rid)) {
                                    Object[] objects = acks.remove((long) rid);
                                    if (objects != null) {
                                        Ack fn = (Ack) objects[1];
                                        if (fn != null) {
                                            fn.call((String) objects[0], object.get("error"), object.get("data"));
                                        } else {
                                            logger.warning("ack function is null with rid " + rid);
                                        }
                                    }
                                }
                                break;
                        }
                    } catch (Exception e) {
                        logger.severe(e.toString());
                    }

                }

            }

            private JsonNode getTextPayload(String payloadText) throws IOException {
                try {
                    return mapper.readTree(payloadText);
                } catch (JsonParseException e) {
                    return mapper.valueToTree(payloadText);
                }
            }


            @Override
            public void onCloseFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {
                logger.warning("On close frame got called");
                super.onCloseFrame(websocket, frame);
            }

            @Override
            public void onSendError(WebSocket websocket, WebSocketException cause, WebSocketFrame frame) throws Exception {
                logger.severe("Error while sending data " + cause.toString());
                super.onSendError(websocket, cause, frame);
            }

        };

    }

    private void putData(ObjectNode node, String key, Object object) {
        if (object instanceof JsonNode) {
            node.set(key, (JsonNode) object);
        } else {
            node.putPOJO(key, (JsonNode) object);
        }
    }

    public Socket emit(final String event, final Object object) {
        EventThread.exec(new Runnable() {
            public void run() {
                ObjectNode eventObject = mapper.createObjectNode();
                eventObject.put("event", event);
                putData(eventObject, "data", object);
                sendData(ws, eventObject);
            }
        });
        return this;
    }


    public Socket emit(final String event, final Object object, final Ack ack) {

        EventThread.exec(new Runnable() {
            public void run() {
                ObjectNode eventObject = mapper.createObjectNode();
                acks.put(counter.longValue(), getAckObject(event, ack));
                eventObject.put("event", event);
                putData(eventObject, "data", object);
                eventObject.put("cid", counter.getAndIncrement());
                sendData(ws, eventObject);
            }
        });
        return this;
    }

    private Socket subscribe(final String channel) {
        EventThread.exec(new Runnable() {
            public void run() {
                ObjectNode subscribeObject = mapper.createObjectNode();
                subscribeObject.put("event", "#subscribe");
                ObjectNode object = mapper.createObjectNode();
                object.put("channel", channel);
                subscribeObject.set("data", object);
                subscribeObject.put("cid", counter.getAndIncrement());
                sendData(ws, subscribeObject);
            }
        });
        return this;
    }

    private Object[] getAckObject(String event, Ack ack) {
        Object object[] = {event, ack};
        return object;
    }

    private Socket subscribe(final String channel, final Ack ack) {
        EventThread.exec(new Runnable() {
            public void run() {
                ObjectNode subscribeObject = mapper.createObjectNode();
                subscribeObject.put("event", "#subscribe");
                ObjectNode object = mapper.createObjectNode();
                acks.put(counter.longValue(), getAckObject(channel, ack));
                object.put("channel", channel);
                putData(subscribeObject, "data", object);
                subscribeObject.put("cid", counter.getAndIncrement());
                sendData(ws, subscribeObject);
            }
        });
        return this;
    }

    private Socket unsubscribe(final String channel) {
        EventThread.exec(new Runnable() {
            public void run() {
                ObjectNode subscribeObject = mapper.createObjectNode();
                subscribeObject.put("event", "#unsubscribe");
                subscribeObject.put("data", channel);
                subscribeObject.put("cid", counter.getAndIncrement());
                sendData(ws, subscribeObject);
            }
        });
        return this;
    }

    private Socket unsubscribe(final String channel, final Ack ack) {
        EventThread.exec(new Runnable() {
            public void run() {
                ObjectNode subscribeObject = mapper.createObjectNode();
                subscribeObject.put("event", "#unsubscribe");
                subscribeObject.put("data", channel);

                acks.put(counter.longValue(), getAckObject(channel, ack));
                subscribeObject.put("cid", counter.getAndIncrement());
                sendData(ws, subscribeObject);
            }
        });
        return this;
    }

    public Socket publish(final String channel, final Object data) {
        EventThread.exec(new Runnable() {
            public void run() {
                ObjectNode publishObject = mapper.createObjectNode();
                publishObject.put("event", "#publish");
                ObjectNode object = mapper.createObjectNode();
                object.put("channel", channel);
                if (data instanceof JsonNode) {
                    object.set("data", (JsonNode) data);
                } else {
                    object.putPOJO("data", data);
                }
                publishObject.put("data", object);
                publishObject.put("cid", counter.getAndIncrement());
                sendData(ws, publishObject);
            }
        });

        return this;
    }

    public Socket publish(final String channel, final Object data, final Ack ack) {
        EventThread.exec(new Runnable() {
            public void run() {
                ObjectNode publishObject = mapper.createObjectNode();
                publishObject.put("event", "#publish");
                ObjectNode object = mapper.createObjectNode();
                acks.put(counter.longValue(), getAckObject(channel, ack));
                object.put("channel", channel);
                putData(object, "data", data);
                publishObject.put("data", object);
                publishObject.put("cid", counter.getAndIncrement());
                sendData(ws, publishObject);
            }
        });

        return this;
    }

    private Ack ack(final Long cid) {
        return new Ack() {
            public void call(final String channel, final Object error, final Object data) {
                EventThread.exec(new Runnable() {
                    public void run() {
                        ObjectNode object = mapper.createObjectNode();
                        putData(object, "error", error);
                        putData(object, "data", data);
                        object.put("rid", cid);
                        sendData(ws, object);
                    }
                });
            }
        };
    }


    private void subscribeChannels() {
        for (Map.Entry<String, Channel> entry : channels.entrySet()) {
            entry.getValue().subscribe();
        }
    }

    public void setExtraHeaders(Map<String, String> extraHeaders, boolean overrideDefaultHeaders) {
        if (overrideDefaultHeaders) {
            headers.clear();
        }

        headers.putAll(extraHeaders);
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void connect() {

        try {
            ws = factory.createSocket(URL);
        } catch (IOException e) {
            logger.severe(e.toString());
        }
        ws.addExtension("permessage-deflate; client_max_window_bits");
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            ws.addHeader(entry.getKey(), entry.getValue());
        }

        ws.addListener(adapter);

        try {
            ws.connect();
        } catch (OpeningHandshakeException e) {
            // A violation against the WebSocket protocol was detected
            // during the opening handshake.

            logger.severe(e.toString());
            // Status line.
            StatusLine sl = e.getStatusLine();
            logger.info("=== Status Line ===");
            logger.info("HTTP Version  = \n" + sl.getHttpVersion());
            logger.info("Status Code   = \n" + sl.getStatusCode());
            logger.info("Reason Phrase = \n" + sl.getReasonPhrase());

            // HTTP headers.
            Map<String, List<String>> headers = e.getHeaders();
            logger.info("=== HTTP Headers ===");
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                // Header name.
                String name = entry.getKey();

                // Values of the header.
                List<String> values = entry.getValue();

                if (values == null || values.size() == 0) {
                    // Print the name only.
                    logger.info(name);
                    continue;
                }

                for (String value : values) {
                    // Print the name and the value.
                    logger.info(name + value + "\n");
                }
            }
        } catch (WebSocketException e) {
            listener.onConnectError(Socket.this, e);
            reconnect();
        }

    }

    public void connectAsync() {
        try {
            ws = factory.createSocket(URL);
        } catch (IOException e) {
            logger.severe(e.toString());
        }
        ws.addExtension("permessage-deflate; client_max_window_bits");
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            ws.addHeader(entry.getKey(), entry.getValue());
        }

        ws.addListener(adapter);
        ws.connectAsynchronously();
    }

    private void reconnect() {
        if (strategy == null) {
            logger.warning("Unable to reconnect: reconnection is null");
            return;
        }

        if (strategy.areAttemptsComplete()) {
            strategy.setAttemptsMade(0);
            logger.warning("Unable to reconnect: max reconnection attempts reached");
            return;
        }

        final Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (strategy == null) {
                    logger.warning("Unable to reconnect: reconnection is null");
                    return;
                }
                strategy.processValues();
                Socket.this.connect();
                timer.cancel();
                timer.purge();
            }
        }, strategy.getReconnectInterval());
    }

    public void disconnect() {
        if (ws != null) {
            ws.disconnect();
        }
        strategy = null;
    }

    /**
     * States can be
     * CLOSED
     * CLOSING
     * CONNECTING
     * CREATED
     * OPEN
     */

    public WebSocketState getCurrentState() {
        return ws != null ? ws.getState() : null;
    }

    public Boolean isconnected() {
        return ws != null && ws.getState() == WebSocketState.OPEN;
    }

    public void disableLogging() {
        logger.setLevel(Level.OFF);
    }

    /**
     * Channels need to be subscribed everytime whenever client is reconnected to server (handled inside)
     * Add only one listener to one channel for whole lifetime of process
     */

    public class Channel {

        String channelName;

        public String getChannelName() {
            return channelName;
        }

        public Channel(String channelName) {
            this.channelName = channelName;
        }

        public void subscribe() {
            Socket.this.subscribe(channelName);
        }

        public void subscribe(Ack ack) {
            Socket.this.subscribe(channelName, ack);
        }

        public void onMessage(Listener listener) {
            Socket.this.onSubscribe(channelName, listener);
        }

        public void publish(Object data) {
            Socket.this.publish(channelName, data);
        }

        public void publish(Object data, Ack ack) {
            Socket.this.publish(channelName, data, ack);
        }

        public void unsubscribe() {
            Socket.this.unsubscribe(channelName);
            channels.remove(this);
        }

        public void unsubscribe(Ack ack) {
            Socket.this.unsubscribe(channelName, ack);
            channels.remove(this);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        ws.disconnect("Client socket garbage collected, closing connection");
        super.finalize();
    }
}
