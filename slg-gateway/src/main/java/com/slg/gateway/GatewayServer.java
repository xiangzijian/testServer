package com.slg.gateway;

import com.slg.common.config.ConfigLoader;
import com.slg.common.config.ServerConfig;
import com.slg.common.discovery.ServiceDiscovery;
import com.slg.common.discovery.ServiceInstance;
import com.slg.common.discovery.ServiceType;
import com.slg.common.discovery.StaticServiceDiscovery;
import com.slg.common.msg.RouteTable;
import com.slg.common.net.client.ClientCodec;
import com.slg.common.net.internal.InternalClient;
import com.slg.gateway.backend.BackendManager;
import com.slg.gateway.net.ClientHandler;
import com.slg.gateway.net.ClientServer;
import com.slg.gateway.session.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gateway 入口：先建立到 world/game 的内部连接（后台自动重连），再开放客户端端口。
 */
public final class GatewayServer {

    private static final Logger log = LoggerFactory.getLogger(GatewayServer.class);

    private final ServiceDiscovery discovery;
    private final SessionManager sessions;
    private final BackendManager backends;
    private final ClientServer clientServer;

    public GatewayServer(ServerConfig config) {
        int nodeId = config.getInt("server.nodeId", 1);
        String host = config.getString("client.host", "0.0.0.0");
        int port = config.getInt("client.port", 9001);
        ServiceInstance self = new ServiceInstance(ServiceType.GATEWAY, nodeId, host, port);

        InternalClient.Options opts = new InternalClient.Options();
        opts.heartbeatSeconds = config.getInt("backend.heartbeatSeconds", 10);
        opts.reconnectMinMillis = config.getLong("backend.reconnectMinMillis", 1_000);
        opts.reconnectMaxMillis = config.getLong("backend.reconnectMaxMillis", 30_000);
        opts.connectTimeoutMillis = config.getInt("backend.connectTimeoutMillis", 3_000);

        this.discovery = new StaticServiceDiscovery(config);
        this.sessions = new SessionManager(nodeId);
        this.backends = new BackendManager(self, discovery, sessions, opts, config.getInt("backend.ioThreads", 0));
        // TODO 路由表从配置/注册中心加载
        ClientHandler handler = new ClientHandler(sessions, backends, RouteTable.defaults(),
                config.getInt("client.maxConnections", 20_000));
        this.clientServer = new ClientServer(host, port,
                config.getInt("client.ioThreads", 0),
                config.getInt("client.readerIdleSeconds", 60),
                config.getInt("client.maxFrameLength", ClientCodec.DEFAULT_MAX_FRAME),
                handler);
    }

    public void start() throws InterruptedException {
        backends.start();
        clientServer.start();
    }

    public int clientPort() {
        return clientServer.boundPort();
    }

    public BackendManager backends() {
        return backends;
    }

    public SessionManager sessions() {
        return sessions;
    }

    public void stop() {
        clientServer.stop();
        backends.stop();
        discovery.close();
        log.info("gateway stopped");
    }

    public static void main(String[] args) throws Exception {
        ServerConfig config = ConfigLoader.load("gateway.properties");
        log.info("loading config from {}", config.source());
        GatewayServer server = new GatewayServer(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "gateway-shutdown"));
        server.start();
    }
}
