package org.example.app.utils;

import org.example.app.controller.AppController;
import org.example.app.service.AppService;
import org.example.app.view.AppView;

public class AppStarter {

    public static void startApp() {
        AppService service = new AppService();
        AppView view = new AppView();
        AppController controller = new AppController(view, service);
        controller.runApp();
    }
}
