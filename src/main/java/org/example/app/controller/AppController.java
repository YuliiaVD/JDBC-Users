package org.example.app.controller;

import org.example.app.service.AppService;
import org.example.app.view.AppView;

public class AppController {
    AppView view;
    AppService service;

    public AppController(AppView view, AppService service) {
        this.view = view;
        this.service = service;
    }

    public void runApp() {
        service.handleOption(view.getOption());
}
}