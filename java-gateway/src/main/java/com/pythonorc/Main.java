package com.pythonorc;

import py4j.GatewayServer;

public class Main {
    public static void main(String[] args) throws Exception {
        GatewayServer server = new GatewayServer();
        server.start();
    }
}
